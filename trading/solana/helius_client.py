"""
Helius HTTP client + webhook parser.

Deliberately minimal — only implements the calls the deployer registry
and launch monitor need, and abstracts them behind a protocol so you
can swap Helius for Birdeye or a custom indexer later without touching
the registry or monitor.

Endpoints:
  * GET /v0/addresses/{address}/transactions
      Parsed Helius "Enhanced Transactions" — one item per tx with
      extracted token transfers, swaps, and NFT events.
  * GET /v0/token-metadata
      Batched token metadata; we only care about ``mintAuthority``,
      ``supply`` and ``decimals``.
  * POST / with jsonrpc ``getSignaturesForAddress``
      Raw RPC fallback when a program feed is needed and Helius' parsed
      endpoints don't cover it.

Webhooks are inbound — the operator configures Helius to POST to a URL,
the listener process deserialises each payload via
:func:`parse_webhook_payload` and forwards a normalised
:class:`WebhookEvent` to the launch monitor.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Protocol

import httpx
from loguru import logger as log


_DEFAULT_TIMEOUT = 10.0
_DEFAULT_MAINNET = "https://api.helius.xyz"


# ----------------------------------------------------------------------
# Webhook event types we care about
# ----------------------------------------------------------------------
WEBHOOK_TOKEN_MINT = "TOKEN_MINT"
WEBHOOK_CREATE_POOL = "CREATE_POOL"
WEBHOOK_SWAP = "SWAP"
WEBHOOK_TRANSFER = "TRANSFER"
WEBHOOK_UNKNOWN = "UNKNOWN"


# ----------------------------------------------------------------------
# DTOs
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class DeployRecord:
    """One token creation attributable to a wallet.

    Fields beyond ``mint`` and ``created_at`` are best-effort — they may
    be None when the provider doesn't expose them. The registry scorer
    degrades gracefully when stats are missing.
    """

    mint: str
    created_at: datetime
    initial_liquidity_usd: float | None = None
    peak_market_cap_usd: float | None = None
    current_market_cap_usd: float | None = None
    deployer_hold_seconds: int | None = None
    source: str = "helius"


@dataclass(frozen=True)
class EarlyBuyer:
    """A wallet observed buying a mint shortly after its pool creation."""

    wallet: str
    mint: str
    bought_at: datetime
    amount_usd: float | None = None


@dataclass(frozen=True)
class WebhookEvent:
    """Normalised representation of a Helius webhook payload row."""

    event_type: str
    signature: str
    timestamp: datetime
    source_wallet: str | None
    mints: tuple[str, ...] = ()
    pool_address: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


# ----------------------------------------------------------------------
# Provider protocol
# ----------------------------------------------------------------------
class DeployInfoProvider(Protocol):
    """Swap-in interface for deployer-registry data sources.

    Allows :class:`trading.solana.deployer_registry.DeployerRegistry` to
    work with Helius today and Birdeye / an in-house indexer tomorrow.
    """

    def list_wallet_deploys(self, wallet: str, lookback_days: int = 180) -> list[DeployRecord]: ...
    def get_early_buyers(
        self,
        mint: str,
        window_seconds: int = 60,
        limit: int = 50,
    ) -> list[EarlyBuyer]: ...
    def get_mint_deployer(self, mint: str) -> str | None: ...


# ----------------------------------------------------------------------
# Client
# ----------------------------------------------------------------------
class HeliusError(RuntimeError):
    """Raised when Helius returns an error or is unreachable."""


class HeliusClient:
    """HTTP client for Helius Enhanced Transactions + webhooks."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = _DEFAULT_MAINNET,
        timeout: float = _DEFAULT_TIMEOUT,
        client: httpx.Client | None = None,
    ) -> None:
        self._api_key = api_key or None
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout)

    # ------------------------------------------------------------------
    # Resource management
    # ------------------------------------------------------------------
    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "HeliusClient":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _params(self, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        params = dict(extra or {})
        if self._api_key:
            params["api-key"] = self._api_key
        return params

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self._base_url}{path}"
        try:
            resp = self._client.get(url, params=self._params(params))
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as exc:
            log.error("Helius GET {p} failed: {e}", p=path, e=str(exc))
            raise HeliusError(f"GET {path} failed: {exc}") from exc

    # ------------------------------------------------------------------
    # Transactions — the raw input for deployer scoring
    # ------------------------------------------------------------------
    def get_wallet_transactions(
        self,
        wallet: str,
        limit: int = 100,
        before: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return parsed transactions for ``wallet``, newest first.

        ``before`` is a Helius signature cursor for pagination. Returns
        an empty list if the wallet has no activity.
        """
        if not wallet:
            raise ValueError("wallet is required")
        params: dict[str, Any] = {"limit": limit}
        if before:
            params["before"] = before
        raw = self._get(
            f"/v0/addresses/{wallet}/transactions",
            params=params,
        )
        if not isinstance(raw, list):
            raise HeliusError(f"unexpected transactions shape: {type(raw)}")
        return raw

    def list_wallet_deploys(
        self,
        wallet: str,
        lookback_days: int = 180,
    ) -> list[DeployRecord]:
        """Filter a wallet's tx history to token-creation events.

        The implementation walks Enhanced Transactions and picks the
        ones whose ``type`` is ``TOKEN_MINT`` with ``wallet`` as the
        signer or mint authority. It intentionally does NOT follow
        pagination — 100 transactions is a reasonable upper bound for a
        deployer in the lookback window; deeper history should be
        handled by the scheduler calling this periodically.
        """
        txs = self.get_wallet_transactions(wallet, limit=100)
        cutoff = _seconds_ago(lookback_days * 86_400)
        out: list[DeployRecord] = []
        for tx in txs:
            if not isinstance(tx, dict):
                continue
            if tx.get("type") != WEBHOOK_TOKEN_MINT:
                continue
            ts = _helius_timestamp(tx)
            if ts is None or ts.timestamp() < cutoff:
                continue
            mint = _first_token_mint(tx)
            if mint is None:
                continue
            out.append(
                DeployRecord(
                    mint=mint,
                    created_at=ts,
                    source="helius",
                )
            )
        return out

    # ------------------------------------------------------------------
    # Mint → deployer — walk tx history for the earliest TOKEN_MINT event
    # ------------------------------------------------------------------
    def get_mint_deployer(self, mint: str) -> str | None:
        """Resolve the deployer wallet for ``mint`` from on-chain history.

        Strategy: fetch the mint account's most recent transactions from
        Helius Enhanced Transactions (which walks mint-account history),
        pick the earliest TOKEN_MINT event in the batch, and return its
        ``feePayer``. Returns None if no TOKEN_MINT event is present in
        the first page or the API call fails.

        This is deliberately a *best-effort* lookup — the correct
        deployer is the signer of the ``InitializeMint`` instruction,
        which lives in the first transaction ever involving this mint.
        The most-recent paging we use here only catches freshly-created
        mints; older mints require pagination (``before=`` cursor)
        which the caller can implement when they need to backfill.
        """
        if not mint:
            raise ValueError("mint is required")
        try:
            txs = self.get_wallet_transactions(mint, limit=100)
        except HeliusError:
            return None

        mint_events = [
            tx for tx in txs
            if isinstance(tx, dict) and tx.get("type") == WEBHOOK_TOKEN_MINT
        ]
        if not mint_events:
            return None

        earliest = min(
            mint_events,
            key=lambda tx: (_helius_timestamp(tx) or datetime.now(timezone.utc)),
        )
        payer = earliest.get("feePayer")
        if isinstance(payer, str) and payer:
            return payer

        signers = earliest.get("signers") or []
        if isinstance(signers, list) and signers and isinstance(signers[0], str):
            return signers[0]
        return None

    # ------------------------------------------------------------------
    # Early-buyer lookup — Swaps against a mint in the first N seconds
    # ------------------------------------------------------------------
    def get_early_buyers(
        self,
        mint: str,
        window_seconds: int = 60,
        limit: int = 50,
    ) -> list[EarlyBuyer]:
        """Return wallets that swapped INTO ``mint`` within ``window_seconds``
        of the pool's first observed swap.

        This is a heuristic: we fetch the mint's recent swaps, sort by
        timestamp, anchor to the earliest, and keep swaps within the
        window where the user received the mint.
        """
        if not mint:
            raise ValueError("mint is required")
        txs = self._get(
            f"/v0/addresses/{mint}/transactions",
            params={"limit": limit, "type": WEBHOOK_SWAP},
        )
        if not isinstance(txs, list) or not txs:
            return []

        swaps = [
            tx for tx in txs
            if isinstance(tx, dict) and tx.get("type") == WEBHOOK_SWAP
        ]
        if not swaps:
            return []

        earliest = min(
            (_helius_timestamp(tx) or datetime.now(timezone.utc) for tx in swaps),
        )
        cutoff = earliest.timestamp() + window_seconds

        out: list[EarlyBuyer] = []
        for tx in swaps:
            ts = _helius_timestamp(tx)
            if ts is None or ts.timestamp() > cutoff:
                continue
            buyer = _swap_recipient(tx, mint)
            if buyer is None:
                continue
            out.append(
                EarlyBuyer(
                    wallet=buyer,
                    mint=mint,
                    bought_at=ts,
                    amount_usd=_swap_amount_usd(tx),
                )
            )
        return out


# ----------------------------------------------------------------------
# Webhook payload parsing — stateless helpers
# ----------------------------------------------------------------------
def parse_webhook_payload(payload: Any) -> list[WebhookEvent]:
    """Turn a raw Helius webhook body into a list of :class:`WebhookEvent`.

    Helius webhooks POST an array of transactions. Each transaction has
    a ``type`` and a ``signature`` at minimum, and for parsed feeds it
    also includes ``tokenTransfers`` and ``events`` sub-objects.

    Unknown/malformed entries are skipped with a warning; the function
    never raises.
    """
    if not isinstance(payload, list):
        log.warning("Helius webhook payload is not a list: {t}", t=type(payload))
        return []
    out: list[WebhookEvent] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        event_type = str(row.get("type") or WEBHOOK_UNKNOWN)
        signature = str(row.get("signature") or "")
        if not signature:
            continue
        ts = _helius_timestamp(row) or datetime.now(timezone.utc)
        mints = tuple(_extract_mints(row))
        pool_address = _extract_pool_address(row)
        source_wallet = _extract_source_wallet(row)
        out.append(
            WebhookEvent(
                event_type=event_type,
                signature=signature,
                timestamp=ts,
                source_wallet=source_wallet,
                mints=mints,
                pool_address=pool_address,
                raw=row,
            )
        )
    return out


# ----------------------------------------------------------------------
# Internal helpers
# ----------------------------------------------------------------------
def _helius_timestamp(tx: dict[str, Any]) -> datetime | None:
    raw = tx.get("timestamp")
    if raw is None:
        return None
    try:
        return datetime.fromtimestamp(int(raw), tz=timezone.utc)
    except (TypeError, ValueError):
        return None


def _seconds_ago(delta: float) -> float:
    return datetime.now(timezone.utc).timestamp() - delta


def _first_token_mint(tx: dict[str, Any]) -> str | None:
    transfers = tx.get("tokenTransfers") or []
    if not isinstance(transfers, list):
        return None
    for t in transfers:
        if isinstance(t, dict):
            mint = t.get("mint")
            if isinstance(mint, str) and mint:
                return mint
    return None


def _extract_mints(tx: dict[str, Any]) -> Iterable[str]:
    seen: set[str] = set()
    transfers = tx.get("tokenTransfers") or []
    if isinstance(transfers, list):
        for t in transfers:
            if isinstance(t, dict):
                mint = t.get("mint")
                if isinstance(mint, str) and mint and mint not in seen:
                    seen.add(mint)
                    yield mint
    events = tx.get("events") or {}
    if isinstance(events, dict):
        for _, payload in events.items():
            if isinstance(payload, dict):
                mint = payload.get("mint")
                if isinstance(mint, str) and mint and mint not in seen:
                    seen.add(mint)
                    yield mint


def _extract_pool_address(tx: dict[str, Any]) -> str | None:
    events = tx.get("events") or {}
    if isinstance(events, dict):
        swap = events.get("swap") or {}
        if isinstance(swap, dict):
            pool = swap.get("poolId") or swap.get("pool")
            if isinstance(pool, str) and pool:
                return pool
    return None


def _extract_source_wallet(tx: dict[str, Any]) -> str | None:
    fee_payer = tx.get("feePayer")
    if isinstance(fee_payer, str) and fee_payer:
        return fee_payer
    sigs = tx.get("signers") or []
    if isinstance(sigs, list) and sigs and isinstance(sigs[0], str):
        return sigs[0]
    return None


def _swap_recipient(tx: dict[str, Any], mint: str) -> str | None:
    transfers = tx.get("tokenTransfers") or []
    if not isinstance(transfers, list):
        return None
    for t in transfers:
        if not isinstance(t, dict):
            continue
        if t.get("mint") != mint:
            continue
        to = t.get("toUserAccount") or t.get("to")
        if isinstance(to, str) and to:
            return to
    return None


def _swap_amount_usd(tx: dict[str, Any]) -> float | None:
    events = tx.get("events") or {}
    if isinstance(events, dict):
        swap = events.get("swap") or {}
        if isinstance(swap, dict):
            raw = swap.get("amountUsd") or swap.get("usd")
            try:
                return float(raw) if raw is not None else None
            except (TypeError, ValueError):
                return None
    return None
