"""
Pre-sign check for Jupiter Ultra orders (#1143).

``SolanaWallet.sign_base64_transaction`` signs whatever bytes Jupiter's
``/order`` endpoint returns. A wrong response (bad config, a compromised
endpoint, a proxy in between) would be signed as-is: an extra transfer out
of the wallet, a different output token, or a swap that pays someone else.

This module checks the order before the signature is applied, using only our
own Solana RPC and no new dependency:

1. Decode the unsigned versioned transaction (legacy and v0) without
   ``solders`` and list every account it can write, including the writable
   entries it loads from address lookup tables. Only writable accounts can
   change balance, so this list is complete.
2. Read those accounts now (``getMultipleAccounts``) and as they would be
   after the transaction (``simulateTransaction`` with ``accounts``,
   ``sigVerify=false``, ``replaceRecentBlockhash=true``).
3. Net the wallet's changes per mint across every token account the wallet
   owns before or after (classic SPL Token and Token-2022), and native SOL
   (wallet lamports plus wrapped-SOL token amounts).
4. Refuse unless: the wallet is a required signer; the simulation succeeds;
   no wallet token account gains a delegate or a foreign close authority and
   the wallet account is not reassigned;
   the output mint rises by at least the order's minimum; the input mint
   falls by no more than the order's input amount; no other mint falls; and
   SOL outside the swap legs falls by no more than a fee/rent allowance.

Every failure, including an RPC failure or an undecodable transaction,
raises :class:`PresignCheckFailed`. Nothing is signed unless the check
passes.
"""

from __future__ import annotations

import base64
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Protocol

from loguru import logger as log


SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EnaFFtpo5ESD6mN91R"
SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"
_TOKEN_PROGRAMS = frozenset({TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID})

# SPL token account layout: mint[0:32], owner[32:64], amount u64 LE [64:72],
# delegate COption<Pubkey> [72:108], state [108], is_native COption<u64>
# [109:121], delegated_amount u64 [121:129], close_authority COption<Pubkey>
# [129:165]. Token-2022 accounts share this base and append extensions.
_TOKEN_ACCOUNT_MIN_LEN = 165
# Address lookup table account: 56-byte meta, then 32-byte addresses.
_LOOKUP_TABLE_META_SIZE = 56

# getMultipleAccounts accepts at most 100 addresses per call.
MAX_ACCOUNTS = 100

# Network fee + priority fee + rent for a few new token accounts
# (~0.00204 SOL each). 0.01 SOL.
DEFAULT_MAX_SOL_FEE_LAMPORTS = 10_000_000

# Used only when the order carries no otherAmountThreshold.
DEFAULT_SLIPPAGE_TOLERANCE_BPS = 300


class PresignCheckFailed(RuntimeError):
    """The order did not pass the pre-sign check; it must not be signed."""


class _RPC(Protocol):
    def get_multiple_accounts(self, addresses: list[str]) -> list[dict[str, Any] | None]: ...

    def simulate_transaction(
        self, b64_tx: str, account_addresses: list[str],
    ) -> dict[str, Any]: ...


# ----------------------------------------------------------------------
# base58 / wire decoding (no solders)
# ----------------------------------------------------------------------
_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58encode(raw: bytes) -> str:
    """Bitcoin-alphabet base58, as used for Solana addresses."""
    n = int.from_bytes(raw, "big")
    out = []
    while n:
        n, rem = divmod(n, 58)
        out.append(_B58_ALPHABET[rem])
    pad = len(raw) - len(raw.lstrip(b"\0"))
    return "1" * pad + "".join(reversed(out))


def _read_compact_u16(buf: bytes, pos: int) -> tuple[int, int]:
    value = 0
    for shift in (0, 7, 14):
        if pos >= len(buf):
            raise PresignCheckFailed("transaction truncated (compact-u16)")
        byte = buf[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
    raise PresignCheckFailed("invalid compact-u16 in transaction")


def _take(buf: bytes, pos: int, n: int) -> tuple[bytes, int]:
    if n < 0 or pos + n > len(buf):
        raise PresignCheckFailed("transaction truncated")
    return buf[pos:pos + n], pos + n


@dataclass(frozen=True)
class AddressTableLookup:
    table: str
    writable_indexes: tuple[int, ...]
    readonly_indexes: tuple[int, ...]


@dataclass(frozen=True)
class DecodedMessage:
    version: int | None  # None = legacy
    num_required_signatures: int
    static_keys: tuple[str, ...]
    static_writable: tuple[str, ...]
    lookups: tuple[AddressTableLookup, ...] = field(default=())

    @property
    def required_signers(self) -> tuple[str, ...]:
        return self.static_keys[: self.num_required_signatures]


def decode_transaction(b64_tx: str) -> DecodedMessage:
    """Decode the account section of a base64 Solana transaction."""
    try:
        buf = base64.b64decode(b64_tx, validate=True)
    except (ValueError, TypeError) as exc:
        raise PresignCheckFailed(f"transaction is not valid base64: {exc}") from exc

    n_sigs, pos = _read_compact_u16(buf, 0)
    _, pos = _take(buf, pos, 64 * n_sigs)

    version: int | None = None
    if pos < len(buf) and buf[pos] & 0x80:
        version = buf[pos] & 0x7F
        pos += 1
        if version != 0:
            raise PresignCheckFailed(f"unsupported message version {version}")

    header, pos = _take(buf, pos, 3)
    n_req, n_ro_signed, n_ro_unsigned = header[0], header[1], header[2]

    n_keys, pos = _read_compact_u16(buf, pos)
    keys: list[str] = []
    for _ in range(n_keys):
        raw, pos = _take(buf, pos, 32)
        keys.append(b58encode(raw))
    if n_req > n_keys or n_ro_signed > n_req or n_ro_unsigned > n_keys - n_req:
        raise PresignCheckFailed("inconsistent message header")

    writable = [
        k for i, k in enumerate(keys)
        if (i < n_req - n_ro_signed) or (n_req <= i < n_keys - n_ro_unsigned)
    ]

    _, pos = _take(buf, pos, 32)  # recent blockhash
    n_ix, pos = _read_compact_u16(buf, pos)
    for _ in range(n_ix):
        _, pos = _take(buf, pos, 1)  # program id index
        n_acc, pos = _read_compact_u16(buf, pos)
        _, pos = _take(buf, pos, n_acc)
        n_data, pos = _read_compact_u16(buf, pos)
        _, pos = _take(buf, pos, n_data)

    lookups: list[AddressTableLookup] = []
    if version == 0:
        n_lookups, pos = _read_compact_u16(buf, pos)
        for _ in range(n_lookups):
            raw, pos = _take(buf, pos, 32)
            n_w, pos = _read_compact_u16(buf, pos)
            w_idx, pos = _take(buf, pos, n_w)
            n_r, pos = _read_compact_u16(buf, pos)
            r_idx, pos = _take(buf, pos, n_r)
            lookups.append(AddressTableLookup(
                table=b58encode(raw),
                writable_indexes=tuple(w_idx),
                readonly_indexes=tuple(r_idx),
            ))

    return DecodedMessage(
        version=version,
        num_required_signatures=n_req,
        static_keys=tuple(keys),
        static_writable=tuple(writable),
        lookups=tuple(lookups),
    )


def _account_data(account: dict[str, Any] | None) -> bytes:
    if not account:
        return b""
    data = account.get("data")
    if isinstance(data, list) and data and data[-1] == "base64":
        return base64.b64decode(data[0])
    if isinstance(data, str):
        return base64.b64decode(data)
    raise PresignCheckFailed("account data not base64-encoded")


def _lookup_table_addresses(account: dict[str, Any] | None, table: str) -> list[str]:
    raw = _account_data(account)
    if len(raw) < _LOOKUP_TABLE_META_SIZE:
        raise PresignCheckFailed(f"address lookup table {table} not found")
    body = raw[_LOOKUP_TABLE_META_SIZE:]
    return [b58encode(body[i:i + 32]) for i in range(0, len(body) - 31, 32)]


def writable_accounts(message: DecodedMessage, rpc: _RPC) -> list[str]:
    """Every account the transaction can write, lookup-table entries included."""
    out = list(message.static_writable)
    if message.lookups:
        tables = rpc.get_multiple_accounts([lk.table for lk in message.lookups])
        if len(tables) != len(message.lookups):
            raise PresignCheckFailed("lookup table read returned the wrong count")
        for lk, acct in zip(message.lookups, tables):
            addrs = _lookup_table_addresses(acct, lk.table)
            for idx in lk.writable_indexes:
                if idx >= len(addrs):
                    raise PresignCheckFailed(
                        f"lookup index {idx} out of range for table {lk.table}"
                    )
                out.append(addrs[idx])
    seen: set[str] = set()
    return [a for a in out if not (a in seen or seen.add(a))]


# ----------------------------------------------------------------------
# Balance netting
# ----------------------------------------------------------------------
def _token_state(account: dict[str, Any] | None) -> tuple[str, str, int] | None:
    """(mint, token owner, amount) for an SPL token account, else None."""
    if not account or account.get("owner") not in _TOKEN_PROGRAMS:
        return None
    raw = _account_data(account)
    if len(raw) < _TOKEN_ACCOUNT_MIN_LEN:
        return None  # a mint or other program account, not a token account
    return b58encode(raw[0:32]), b58encode(raw[32:64]), int.from_bytes(raw[64:72], "little")


def _coption_key(raw: bytes, start: int) -> str | None:
    tag = int.from_bytes(raw[start:start + 4], "little")
    return b58encode(raw[start + 4:start + 36]) if tag == 1 else None


def _authorities(account: dict[str, Any] | None) -> tuple[str | None, int, str | None]:
    """(delegate, delegated_amount, close_authority) of a token account."""
    raw = _account_data(account)
    return (
        _coption_key(raw, 72),
        int.from_bytes(raw[121:129], "little"),
        _coption_key(raw, 129),
    )


def authority_changes(
    wallet: str,
    addresses: list[str],
    pre: list[dict[str, Any] | None],
    post: list[dict[str, Any] | None],
) -> list[str]:
    """Changes that move control without moving balance.

    * the wallet account assigned to a program other than System;
    * a wallet token account gaining a delegate, a larger delegated amount,
      or a new close authority.
    """
    problems: list[str] = []
    for addr, before, after in zip(addresses, pre, post):
        if addr == wallet:
            owner = (after or {}).get("owner", SYSTEM_PROGRAM_ID)
            if owner != SYSTEM_PROGRAM_ID:
                problems.append(f"wallet account reassigned to program {owner}")
            continue
        a_state = _token_state(after)
        if not (a_state and a_state[1] == wallet):
            continue
        b_state = _token_state(before)
        b_auth = _authorities(before) if b_state and b_state[1] == wallet else (None, 0, None)
        a_delegate, a_amount, a_close = _authorities(after)
        if a_delegate is not None and (a_delegate != b_auth[0] or a_amount > b_auth[1]):
            problems.append(f"token account {addr} delegates {a_amount} to {a_delegate}")
        if a_close is not None and a_close != b_auth[2] and a_close != wallet:
            problems.append(f"token account {addr} close authority set to {a_close}")
    return problems


def wallet_deltas(
    wallet: str,
    addresses: list[str],
    pre: list[dict[str, Any] | None],
    post: list[dict[str, Any] | None],
) -> dict[str, int]:
    """Net change per mint for the wallet; SOL_MINT includes native lamports."""
    if not (len(addresses) == len(pre) == len(post)):
        raise PresignCheckFailed("account state count mismatch")
    deltas: dict[str, int] = defaultdict(int)
    for addr, before, after in zip(addresses, pre, post):
        if addr == wallet:
            deltas[SOL_MINT] += int((after or {}).get("lamports", 0)) - int(
                (before or {}).get("lamports", 0)
            )
            continue
        b_state, a_state = _token_state(before), _token_state(after)
        # Ownership is judged on each side separately: an account whose
        # owner moves away from the wallet counts as a full loss.
        if b_state and b_state[1] == wallet:
            deltas[b_state[0]] -= b_state[2]
        if a_state and a_state[1] == wallet:
            deltas[a_state[0]] += a_state[2]
    return dict(deltas)


# ----------------------------------------------------------------------
# Public entry point
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class PresignReport:
    wallet: str
    deltas: dict[str, int]
    writable_count: int
    min_output_atoms: int
    max_input_atoms: int


def _min_output_atoms(order: Any, slippage_tolerance_bps: int) -> int:
    raw = getattr(order, "raw", None) or {}
    threshold = raw.get("otherAmountThreshold")
    if threshold not in (None, "", "0", 0):
        try:
            return int(threshold)
        except (TypeError, ValueError) as exc:
            raise PresignCheckFailed(f"bad otherAmountThreshold {threshold!r}") from exc
    out_amount = int(getattr(order, "out_amount", 0) or 0)
    if out_amount <= 0:
        raise PresignCheckFailed("order has no expected output amount")
    return out_amount * (10_000 - slippage_tolerance_bps) // 10_000


def verify_order_before_signing(
    order: Any,
    wallet_address: str,
    rpc: _RPC,
    *,
    max_sol_fee_lamports: int = DEFAULT_MAX_SOL_FEE_LAMPORTS,
    slippage_tolerance_bps: int = DEFAULT_SLIPPAGE_TOLERANCE_BPS,
) -> PresignReport:
    """Raise :class:`PresignCheckFailed` unless the order does what it says.

    ``order`` is a :class:`trading.solana.jupiter_client.SwapOrder`.
    """
    if not wallet_address:
        raise PresignCheckFailed("wallet address is required")
    in_mint, out_mint = order.input_mint, order.output_mint
    if in_mint == out_mint:
        raise PresignCheckFailed("input and output mint are the same")
    max_input = int(order.in_amount)
    if max_input <= 0:
        raise PresignCheckFailed("order has no input amount")
    min_output = _min_output_atoms(order, slippage_tolerance_bps)

    message = decode_transaction(order.transaction)
    if wallet_address not in message.required_signers:
        raise PresignCheckFailed("wallet is not a required signer of the order")

    try:
        addresses = writable_accounts(message, rpc)
        if len(addresses) > MAX_ACCOUNTS:
            raise PresignCheckFailed(
                f"{len(addresses)} writable accounts exceeds the {MAX_ACCOUNTS} the check can read"
            )
        pre = rpc.get_multiple_accounts(addresses)
        sim = rpc.simulate_transaction(order.transaction, addresses)
    except PresignCheckFailed:
        raise
    except Exception as exc:  # noqa: BLE001 - any RPC failure blocks signing
        raise PresignCheckFailed(f"pre-sign RPC failed: {exc}") from exc

    if sim.get("err") is not None:
        raise PresignCheckFailed(f"simulation failed: {sim.get('err')}")
    post = sim.get("accounts")
    if not isinstance(post, list):
        raise PresignCheckFailed("simulation returned no account states")

    deltas = wallet_deltas(wallet_address, addresses, pre, post)

    problems: list[str] = authority_changes(wallet_address, addresses, pre, post)
    fee = max(0, int(max_sol_fee_lamports))
    sol_delta = deltas.get(SOL_MINT, 0)

    out_delta = deltas.get(out_mint, 0)
    out_floor = min_output - (fee if out_mint == SOL_MINT else 0)
    if out_delta < out_floor:
        problems.append(f"output {out_mint} changes by {out_delta}, needs >= {out_floor}")

    in_delta = deltas.get(in_mint, 0)
    in_ceiling = max_input + (fee if in_mint == SOL_MINT else 0)
    if -in_delta > in_ceiling:
        problems.append(f"input {in_mint} falls by {-in_delta}, allowed <= {in_ceiling}")

    if SOL_MINT not in (in_mint, out_mint) and -sol_delta > fee:
        problems.append(f"SOL falls by {-sol_delta} lamports, fee allowance {fee}")

    for mint, delta in sorted(deltas.items()):
        if mint in (in_mint, out_mint, SOL_MINT):
            continue
        if delta < 0:
            problems.append(f"unrelated mint {mint} falls by {-delta}")

    if problems:
        log.warning(
            "Jupiter order {r} refused before signing: {p}",
            r=getattr(order, "request_id", "?"), p="; ".join(problems),
        )
        raise PresignCheckFailed("; ".join(problems))

    return PresignReport(
        wallet=wallet_address,
        deltas=deltas,
        writable_count=len(addresses),
        min_output_atoms=min_output,
        max_input_atoms=max_input,
    )
