"""Tests for trading/solana/presign_check.py (#1143).

Synthetic legacy/v0 transactions and SPL token-account bytes are built in
pure Python, so these tests need neither ``solders`` nor network access.
"""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

import pytest

from trading.solana import presign_check as pc
from trading.solana.presign_check import (
    SOL_MINT,
    SYSTEM_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
    PresignCheckFailed,
    b58encode,
    decode_transaction,
    verify_order_before_signing,
)


# ----------------------------------------------------------------------
# Builders
# ----------------------------------------------------------------------
def _key(name: str) -> bytes:
    return hashlib.sha256(name.encode()).digest()


def _addr(name: str) -> str:
    return b58encode(_key(name))


def _cu16(n: int) -> bytes:
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _tx(
    keys: list[bytes],
    header: tuple[int, int, int],
    lookups: list[tuple[bytes, list[int], list[int]]] | None = None,
    *,
    legacy: bool = False,
) -> str:
    msg = bytearray()
    if not legacy:
        msg.append(0x80)  # v0
    msg += bytes(header)
    msg += _cu16(len(keys)) + b"".join(keys)
    msg += b"\x07" * 32  # blockhash
    # one instruction: program = last key, accounts 0..1, 4 bytes of data
    msg += _cu16(1) + bytes([len(keys) - 1]) + _cu16(2) + bytes([0, 1]) + _cu16(4) + b"\x01\x02\x03\x04"
    if not legacy:
        lookups = lookups or []
        msg += _cu16(len(lookups))
        for table, w, r in lookups:
            msg += table + _cu16(len(w)) + bytes(w) + _cu16(len(r)) + bytes(r)
    n_sigs = header[0]
    raw = _cu16(n_sigs) + b"\0" * 64 * n_sigs + bytes(msg)
    return base64.b64encode(raw).decode()


def _token_account(
    mint: str | bytes,
    owner: str | bytes,
    amount: int,
    *,
    delegate: bytes | None = None,
    delegated: int = 0,
    close_authority: bytes | None = None,
    lamports: int = 2_039_280,
) -> dict[str, Any]:
    mint_b = mint if isinstance(mint, bytes) else _raw_for(mint)
    owner_b = owner if isinstance(owner, bytes) else _raw_for(owner)
    data = bytearray(165)
    data[0:32] = mint_b
    data[32:64] = owner_b
    data[64:72] = amount.to_bytes(8, "little")
    if delegate is not None:
        data[72:76] = (1).to_bytes(4, "little")
        data[76:108] = delegate
    data[108] = 1  # initialized
    data[121:129] = delegated.to_bytes(8, "little")
    if close_authority is not None:
        data[129:133] = (1).to_bytes(4, "little")
        data[133:165] = close_authority
    return {
        "lamports": lamports,
        "owner": TOKEN_PROGRAM_ID,
        "data": [base64.b64encode(bytes(data)).decode(), "base64"],
    }


_RAW: dict[str, bytes] = {}


def _raw_for(addr: str) -> bytes:
    return _RAW[addr]


def _name(name: str) -> tuple[bytes, str]:
    raw = _key(name)
    addr = b58encode(raw)
    _RAW[addr] = raw
    return raw, addr


WALLET_RAW, WALLET = _name("wallet")
ATTACKER_RAW, ATTACKER = _name("attacker")
USDC_RAW, USDC = _name("usdc-mint")
TOKEN_RAW, TOKEN = _name("token-mint")
OTHER_RAW, OTHER = _name("other-mint")
USDC_ACCT_RAW, USDC_ACCT = _name("wallet-usdc-ata")
TOKEN_ACCT_RAW, TOKEN_ACCT = _name("wallet-token-ata")
OTHER_ACCT_RAW, OTHER_ACCT = _name("wallet-other-ata")
WSOL_ACCT_RAW, WSOL_ACCT = _name("wallet-wsol-ata")
POOL_RAW, POOL = _name("pool-vault")
PROGRAM_RAW, PROGRAM = _name("jupiter-program")
TABLE_RAW, TABLE = _name("lookup-table")


def _sol_mint_raw() -> bytes:
    # decode base58 of SOL_MINT via brute big-int (test-only helper)
    n = 0
    for ch in SOL_MINT:
        n = n * 58 + pc._B58_ALPHABET.index(ch)
    return n.to_bytes(32, "big")


_RAW[SOL_MINT] = _sol_mint_raw()


def _wallet_account(lamports: int, owner: str = SYSTEM_PROGRAM_ID) -> dict[str, Any]:
    return {"lamports": lamports, "owner": owner, "data": ["", "base64"]}


@dataclass
class FakeRPC:
    """Pre-state by address; post-state by address; optional lookup tables."""

    pre: dict[str, dict[str, Any] | None]
    post: dict[str, dict[str, Any] | None]
    tables: dict[str, dict[str, Any]] = field(default_factory=dict)
    sim_err: Any = None
    simulated: list[list[str]] = field(default_factory=list)

    def get_multiple_accounts(self, addresses: list[str]) -> list[dict[str, Any] | None]:
        return [self.tables.get(a) or self.pre.get(a) for a in addresses]

    def simulate_transaction(self, b64_tx: str, account_addresses: list[str]) -> dict[str, Any]:
        self.simulated.append(list(account_addresses))
        return {
            "err": self.sim_err,
            "logs": [],
            "accounts": [self.post.get(a) for a in account_addresses],
        }


@dataclass(frozen=True)
class Order:
    transaction: str
    input_mint: str
    output_mint: str
    in_amount: int
    out_amount: int
    raw: dict[str, Any]
    request_id: str = "req-test"


# Static keys: wallet (signer, writable), 3 token accounts + pool (writable),
# program (readonly). header = (1 required sig, 0 ro signed, 1 ro unsigned).
_STATIC = [WALLET_RAW, USDC_ACCT_RAW, TOKEN_ACCT_RAW, OTHER_ACCT_RAW, POOL_RAW, PROGRAM_RAW]
_HEADER = (1, 0, 1)


def _swap_tx() -> str:
    return _tx(_STATIC, _HEADER)


def _base_pre() -> dict[str, dict[str, Any] | None]:
    return {
        WALLET: _wallet_account(1_000_000_000),
        USDC_ACCT: _token_account(USDC, WALLET, 5_000_000),
        TOKEN_ACCT: None,  # created by the swap
        OTHER_ACCT: _token_account(OTHER, WALLET, 777),
        POOL: _token_account(TOKEN, ATTACKER, 10**12),
    }


def _good_post() -> dict[str, dict[str, Any] | None]:
    return {
        WALLET: _wallet_account(1_000_000_000 - 5_000 - 2_039_280),
        USDC_ACCT: _token_account(USDC, WALLET, 4_000_000),
        TOKEN_ACCT: _token_account(TOKEN, WALLET, 500),
        OTHER_ACCT: _token_account(OTHER, WALLET, 777),
        POOL: _token_account(TOKEN, ATTACKER, 10**12 - 500),
    }


def _order(tx: str | None = None, **kw: Any) -> Order:
    defaults = dict(
        transaction=tx or _swap_tx(),
        input_mint=USDC,
        output_mint=TOKEN,
        in_amount=1_000_000,
        out_amount=510,
        raw={"otherAmountThreshold": "495"},
    )
    defaults.update(kw)
    return Order(**defaults)


# ----------------------------------------------------------------------
# Decoding
# ----------------------------------------------------------------------
def test_b58_known_vector():
    assert b58encode(bytes(32)) == SYSTEM_PROGRAM_ID


def test_decode_v0_writable_and_signers():
    msg = decode_transaction(_swap_tx())
    assert msg.version == 0
    assert msg.required_signers == (WALLET,)
    assert msg.static_writable == (WALLET, USDC_ACCT, TOKEN_ACCT, OTHER_ACCT, POOL)


def test_decode_legacy_message():
    msg = decode_transaction(_tx(_STATIC, _HEADER, legacy=True))
    assert msg.version is None
    assert PROGRAM not in msg.static_writable
    assert msg.lookups == ()


@pytest.mark.parametrize("bad", ["", "!!!not base64", base64.b64encode(b"\x01").decode()])
def test_decode_rejects_garbage(bad):
    with pytest.raises(PresignCheckFailed):
        decode_transaction(bad)


# ----------------------------------------------------------------------
# verify_order_before_signing
# ----------------------------------------------------------------------
def test_good_swap_passes():
    rpc = FakeRPC(pre=_base_pre(), post=_good_post())
    report = verify_order_before_signing(_order(), WALLET, rpc)
    assert report.deltas[USDC] == -1_000_000
    assert report.deltas[TOKEN] == 500
    assert report.min_output_atoms == 495
    assert rpc.simulated == [[WALLET, USDC_ACCT, TOKEN_ACCT, OTHER_ACCT, POOL]]


def test_extra_sol_transfer_out_is_refused():
    post = _good_post()
    post[WALLET] = _wallet_account(1_000_000_000 - 500_000_000)  # 0.5 SOL gone
    with pytest.raises(PresignCheckFailed, match="SOL falls"):
        verify_order_before_signing(_order(), WALLET, FakeRPC(_base_pre(), post))


def test_output_paid_to_someone_else_is_refused():
    post = _good_post()
    post[TOKEN_ACCT] = _token_account(TOKEN, ATTACKER, 500)
    with pytest.raises(PresignCheckFailed, match="output"):
        verify_order_before_signing(_order(), WALLET, FakeRPC(_base_pre(), post))


def test_different_output_token_is_refused():
    post = _good_post()
    post[TOKEN_ACCT] = _token_account(OTHER, WALLET, 500)
    with pytest.raises(PresignCheckFailed, match="output"):
        verify_order_before_signing(_order(), WALLET, FakeRPC(_base_pre(), post))


def test_output_below_threshold_is_refused():
    post = _good_post()
    post[TOKEN_ACCT] = _token_account(TOKEN, WALLET, 100)
    with pytest.raises(PresignCheckFailed, match="needs >= 495"):
        verify_order_before_signing(_order(), WALLET, FakeRPC(_base_pre(), post))


def test_input_overspend_is_refused():
    post = _good_post()
    post[USDC_ACCT] = _token_account(USDC, WALLET, 0)  # all 5 USDC, order said 1
    with pytest.raises(PresignCheckFailed, match="input"):
        verify_order_before_signing(_order(), WALLET, FakeRPC(_base_pre(), post))


def test_unrelated_token_drained_is_refused():
    post = _good_post()
    post[OTHER_ACCT] = _token_account(OTHER, WALLET, 0)
    with pytest.raises(PresignCheckFailed, match="unrelated mint"):
        verify_order_before_signing(_order(), WALLET, FakeRPC(_base_pre(), post))


def test_token_account_ownership_moved_is_refused():
    post = _good_post()
    post[OTHER_ACCT] = _token_account(OTHER, ATTACKER, 777)  # SetAuthority(owner)
    with pytest.raises(PresignCheckFailed, match="unrelated mint"):
        verify_order_before_signing(_order(), WALLET, FakeRPC(_base_pre(), post))


def test_new_delegate_is_refused():
    post = _good_post()
    post[OTHER_ACCT] = _token_account(OTHER, WALLET, 777, delegate=ATTACKER_RAW, delegated=777)
    with pytest.raises(PresignCheckFailed, match="delegates"):
        verify_order_before_signing(_order(), WALLET, FakeRPC(_base_pre(), post))


def test_foreign_close_authority_is_refused():
    post = _good_post()
    post[TOKEN_ACCT] = _token_account(TOKEN, WALLET, 500, close_authority=ATTACKER_RAW)
    with pytest.raises(PresignCheckFailed, match="close authority"):
        verify_order_before_signing(_order(), WALLET, FakeRPC(_base_pre(), post))


def test_wallet_reassigned_is_refused():
    post = _good_post()
    post[WALLET] = _wallet_account(1_000_000_000 - 5_000 - 2_039_280, owner=PROGRAM)
    with pytest.raises(PresignCheckFailed, match="reassigned"):
        verify_order_before_signing(_order(), WALLET, FakeRPC(_base_pre(), post))


def test_wallet_not_a_signer_is_refused():
    rpc = FakeRPC(pre=_base_pre(), post=_good_post())
    with pytest.raises(PresignCheckFailed, match="required signer"):
        verify_order_before_signing(_order(), ATTACKER, rpc)
    assert rpc.simulated == []


def test_simulation_error_is_refused():
    rpc = FakeRPC(pre=_base_pre(), post=_good_post(), sim_err={"InstructionError": [0, "Custom"]})
    with pytest.raises(PresignCheckFailed, match="simulation failed"):
        verify_order_before_signing(_order(), WALLET, rpc)


def test_rpc_failure_is_refused():
    rpc = MagicMock()
    rpc.get_multiple_accounts.side_effect = RuntimeError("timeout")
    with pytest.raises(PresignCheckFailed, match="RPC failed"):
        verify_order_before_signing(_order(), WALLET, rpc)


def test_missing_threshold_falls_back_to_quoted_out_with_tolerance():
    rpc = FakeRPC(pre=_base_pre(), post=_good_post())
    report = verify_order_before_signing(_order(raw={}, out_amount=510), WALLET, rpc)
    assert report.min_output_atoms == 510 * 9_700 // 10_000


def test_no_expected_output_is_refused():
    rpc = FakeRPC(pre=_base_pre(), post=_good_post())
    with pytest.raises(PresignCheckFailed, match="expected output"):
        verify_order_before_signing(_order(raw={}, out_amount=0), WALLET, rpc)


def test_lookup_table_writable_accounts_are_checked():
    # The drained account is reachable only through the lookup table.
    static = [WALLET_RAW, USDC_ACCT_RAW, TOKEN_ACCT_RAW, PROGRAM_RAW]
    table_data = bytes(56) + POOL_RAW + OTHER_ACCT_RAW
    tables = {TABLE: {"lamports": 1, "owner": "AddressLookupTab1e1111111111111111111111111",
                      "data": [base64.b64encode(table_data).decode(), "base64"]}}
    tx = _tx(static, (1, 0, 1), lookups=[(TABLE_RAW, [0, 1], [])])
    post = _good_post()
    post[OTHER_ACCT] = _token_account(OTHER, WALLET, 0)
    rpc = FakeRPC(pre=_base_pre(), post=post, tables=tables)

    with pytest.raises(PresignCheckFailed, match="unrelated mint"):
        verify_order_before_signing(_order(tx), WALLET, rpc)
    assert OTHER_ACCT in rpc.simulated[0]


def test_sol_input_swap_with_wrap_and_rent_passes():
    pre = {
        WALLET: _wallet_account(2_000_000_000),
        USDC_ACCT: _token_account(USDC, WALLET, 0),
        TOKEN_ACCT: None,
        OTHER_ACCT: None,  # wSOL ATA slot, created and closed in the tx
        POOL: _token_account(USDC, ATTACKER, 10**12),
    }
    post = {
        WALLET: _wallet_account(2_000_000_000 - 1_000_000_000 - 5_000),
        USDC_ACCT: _token_account(USDC, WALLET, 150_000_000),
        TOKEN_ACCT: None,
        OTHER_ACCT: None,
        POOL: _token_account(USDC, ATTACKER, 10**12 - 150_000_000),
    }
    order = _order(input_mint=SOL_MINT, output_mint=USDC, in_amount=1_000_000_000,
                   out_amount=150_000_000, raw={"otherAmountThreshold": "148000000"})
    report = verify_order_before_signing(order, WALLET, FakeRPC(pre, post))
    assert report.deltas[SOL_MINT] == -1_000_005_000


def test_too_many_writable_accounts_is_refused(monkeypatch):
    monkeypatch.setattr(pc, "MAX_ACCOUNTS", 3)
    with pytest.raises(PresignCheckFailed, match="writable accounts"):
        verify_order_before_signing(_order(), WALLET, FakeRPC(_base_pre(), _good_post()))


# ----------------------------------------------------------------------
# execute_swap integration
# ----------------------------------------------------------------------
def test_execute_swap_without_verifier_never_signs():
    import httpx

    from trading.solana.jupiter_client import JupiterClient, SwapOrder
    from trading.solana.wallet import SolanaWallet

    http = MagicMock(spec=httpx.Client)
    wallet = MagicMock(spec=SolanaWallet)
    wallet.address = WALLET
    order = SwapOrder("req", "TX", USDC, TOKEN, 1, 1, {})

    with pytest.raises(PresignCheckFailed, match="pre-sign verifier"):
        JupiterClient(client=http).execute_swap(order, wallet)
    wallet.sign_base64_transaction.assert_not_called()
    http.post.assert_not_called()


def test_execute_swap_refused_by_verifier_never_signs():
    import httpx

    from trading.solana.jupiter_client import JupiterClient, SwapOrder
    from trading.solana.wallet import SolanaWallet

    http = MagicMock(spec=httpx.Client)
    wallet = MagicMock(spec=SolanaWallet)
    wallet.address = WALLET
    order = SwapOrder("req", "TX", USDC, TOKEN, 1, 1, {})

    def refuse(_order: Any, _addr: str) -> None:
        raise PresignCheckFailed("nope")

    with pytest.raises(PresignCheckFailed, match="nope"):
        JupiterClient(client=http).execute_swap(order, wallet, verify=refuse)
    wallet.sign_base64_transaction.assert_not_called()
    http.post.assert_not_called()


def test_rpc_methods_shape():
    from trading.solana.solana_rpc import SolanaRPC

    rpc = SolanaRPC(client=MagicMock())
    rpc._call = MagicMock(return_value={"value": [None, {"lamports": 1}]})  # type: ignore[method-assign]
    assert rpc.get_multiple_accounts(["a", "b"]) == [None, {"lamports": 1}]
    method, params = rpc._call.call_args.args
    assert method == "getMultipleAccounts" and params[1]["encoding"] == "base64"

    rpc._call = MagicMock(return_value={"value": {"err": None, "accounts": []}})  # type: ignore[method-assign]
    assert rpc.simulate_transaction("TX", ["a"]) == {"err": None, "accounts": []}
    method, params = rpc._call.call_args.args
    assert method == "simulateTransaction"
    assert params[1]["sigVerify"] is False
    assert params[1]["replaceRecentBlockhash"] is True
    assert params[1]["accounts"]["addresses"] == ["a"]
