"""
Solana top-volume universe snapshotter.

Every 4 hours (or whenever the scheduler calls :meth:`ingest_once`):

  1. Pull a candidate universe from Jupiter's strict token list
     (~1000 verified Solana tokens).
  2. Fetch 24h volume and pair stats for each candidate via DexScreener,
     aggregating across all of a token's pairs.
  3. Sort by 24h USD volume and take the top N (default 250).
  4. Write one row per token per snapshot into ``solana_token_universe``.
  5. For any mint we've never seen before, call the enrichment path
     (safety check + optional deployer resolve) and cache the result
     in ``solana_mint_enrichment`` so subsequent snapshots skip the
     external calls.

Design rules:

  * **Stateless provider protocol.** ``TopVolumeProvider`` is the
    abstraction so Jupiter+DexScreener today, Birdeye / CoinGecko
    tomorrow — the ingestor doesn't care.
  * **One worker, sequential.** Good enough for 250 tokens / 4h
    cadence. When we 10x the universe or cut the cadence we split
    into snapshotter + enricher workers behind the same interfaces.
  * **Idempotent and rate-limit safe.** Every external call is
    wrapped in try/except so one bad response doesn't kill the run,
    and DexScreener calls are throttled to the documented 300 req/min
    with plenty of headroom.
  * **Nothing writes to ``paper_trades`` or the exit manager.** This
    module populates *registries*; it never trades. Traders consume
    the universe via ``solana_token_universe`` queries.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Protocol

import httpx
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from trading.solana.deployer_registry import DeployerRegistry
from trading.solana.helius_client import DeployInfoProvider
from trading.solana.safety import SolanaSafetyChecker, TokenSafetyReport


# ----------------------------------------------------------------------
# Constants — DexScreener / Jupiter
# ----------------------------------------------------------------------
_DEX_BASE = "https://api.dexscreener.com"
_JUPITER_STRICT_URL = "https://token.jup.ag/strict"
_DEX_BATCH_SIZE = 30               # DexScreener tokens endpoint limit
_DEX_REQUEST_DELAY = 0.25          # 4 req/s = 240 req/min (under 300 cap)
_DEFAULT_HTTP_TIMEOUT = 15.0


# ----------------------------------------------------------------------
# DTOs
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class TokenVolumeSnapshot:
    """One row of top-volume data — one token at one point in time."""

    mint: str
    symbol: str | None
    name: str | None
    volume_24h_usd: float
    price_usd: float | None
    market_cap_usd: float | None
    liquidity_usd: float | None
    pair_count: int
    top_pair: str | None
    source: str = "dexscreener"
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class IngestSummary:
    """Result of a single :meth:`TopVolumeIngestor.ingest_once` run."""

    snapshot_at: datetime
    tokens_considered: int = 0
    tokens_written: int = 0
    new_mints_enriched: int = 0
    enrichment_errors: int = 0
    http_errors: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_at": self.snapshot_at.isoformat(),
            "tokens_considered": self.tokens_considered,
            "tokens_written": self.tokens_written,
            "new_mints_enriched": self.new_mints_enriched,
            "enrichment_errors": self.enrichment_errors,
            "http_errors": self.http_errors,
        }


# ----------------------------------------------------------------------
# Provider protocol — tests + future Birdeye/CoinGecko swap-ins
# ----------------------------------------------------------------------
class TopVolumeProvider(Protocol):
    def list_top_by_volume(self, limit: int) -> list[TokenVolumeSnapshot]: ...


# ----------------------------------------------------------------------
# Jupiter + DexScreener concrete provider
# ----------------------------------------------------------------------
class JupiterDexScreenerProvider:
    """Fetch top-N-by-volume Solana tokens via Jupiter + DexScreener."""

    def __init__(
        self,
        jupiter_tokens_url: str = _JUPITER_STRICT_URL,
        dex_base_url: str = _DEX_BASE,
        batch_size: int = _DEX_BATCH_SIZE,
        request_delay: float = _DEX_REQUEST_DELAY,
        timeout: float = _DEFAULT_HTTP_TIMEOUT,
        client: httpx.Client | None = None,
    ) -> None:
        self._jupiter_url = jupiter_tokens_url
        self._dex_base = dex_base_url.rstrip("/")
        self._batch_size = max(1, min(batch_size, _DEX_BATCH_SIZE))
        self._delay = max(0.0, request_delay)
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout)
        # Simple counter so the summary can report HTTP failures.
        self.http_errors = 0

    # ------------------------------------------------------------------
    # Resource management
    # ------------------------------------------------------------------
    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "JupiterDexScreenerProvider":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def list_top_by_volume(self, limit: int) -> list[TokenVolumeSnapshot]:
        universe = self._fetch_jupiter_universe()
        if not universe:
            log.warning("Jupiter strict list was empty; universe is empty")
            return []

        log.info(
            "JupiterDexScreenerProvider: querying DexScreener for {n} tokens",
            n=len(universe),
        )
        # Mutable accumulator per mint — frozen into dataclasses below.
        accum: dict[str, _MintAccumulator] = {}
        for batch in _batched(universe, self._batch_size):
            pairs = self._fetch_dex_batch([t["address"] for t in batch])
            if self._delay:
                time.sleep(self._delay)
            for pair in pairs:
                _fold_pair(pair, accum)

        snapshots = [a.to_snapshot() for a in accum.values()]
        snapshots.sort(key=lambda s: s.volume_24h_usd, reverse=True)
        return snapshots[:limit]

    # ------------------------------------------------------------------
    # Internal — Jupiter
    # ------------------------------------------------------------------
    def _fetch_jupiter_universe(self) -> list[dict[str, Any]]:
        try:
            resp = self._client.get(self._jupiter_url)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as exc:
            # Network/HTTP errors here are transient (DNS hiccups, 5xx, rate
            # limits).  Log at WARNING — the next 4-hour cycle will retry.
            # Real bugs in this code path raise non-HTTPError exceptions and
            # surface via the outer ingest_once handler at line ~331.
            self.http_errors += 1
            log.warning("Jupiter strict list fetch failed: {e}", e=str(exc))
            return []
        if not isinstance(data, list):
            log.warning("Jupiter strict list unexpected shape: {t}", t=type(data))
            return []
        return [
            {"address": t["address"], "symbol": t.get("symbol"), "name": t.get("name")}
            for t in data
            if isinstance(t, dict) and isinstance(t.get("address"), str)
        ]

    # ------------------------------------------------------------------
    # Internal — DexScreener
    # ------------------------------------------------------------------
    def _fetch_dex_batch(self, addresses: list[str]) -> list[dict[str, Any]]:
        if not addresses:
            return []
        url = f"{self._dex_base}/latest/dex/tokens/{','.join(addresses)}"
        try:
            resp = self._client.get(url)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as exc:
            self.http_errors += 1
            log.warning(
                "DexScreener batch failed ({n} tokens): {e}",
                n=len(addresses), e=str(exc),
            )
            return []
        pairs = data.get("pairs") if isinstance(data, dict) else None
        if not isinstance(pairs, list):
            return []
        return [p for p in pairs if isinstance(p, dict)]


# ----------------------------------------------------------------------
# Ingestor — writes universe snapshots and (optionally) enriches new mints
# ----------------------------------------------------------------------
class TopVolumeIngestor:
    """Pull top-N by volume and persist one snapshot row per token.

    The ingestor is deliberately passive: it only writes to
    ``solana_token_universe`` and ``solana_mint_enrichment``. It never
    opens trades, never touches the exit manager, and never calls the
    LLM router.
    """

    def __init__(
        self,
        engine: Engine,
        provider: TopVolumeProvider,
        safety: SolanaSafetyChecker | None = None,
        deployer_registry: DeployerRegistry | None = None,
        deploy_provider: DeployInfoProvider | None = None,
        limit: int = 250,
        enrich_on_insert: bool = True,
        clock: Any = None,
    ) -> None:
        self.engine = engine
        self.provider = provider
        self.safety = safety
        self.deployer_registry = deployer_registry
        # ``deploy_provider`` is the source we use to translate mint → deployer
        # wallet. It's separate from ``deployer_registry`` (the score store)
        # so the two can be mocked independently in tests and swapped for
        # alternative indexers in production.
        self.deploy_provider = deploy_provider
        self.limit = limit
        self.enrich_on_insert = enrich_on_insert
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._ensure_tables()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    def _ensure_tables(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS solana_token_universe (
                        id               SERIAL PRIMARY KEY,
                        mint             TEXT NOT NULL,
                        symbol           TEXT,
                        name             TEXT,
                        rank             INTEGER NOT NULL,
                        volume_24h_usd   FLOAT NOT NULL,
                        price_usd        FLOAT,
                        market_cap_usd   FLOAT,
                        liquidity_usd    FLOAT,
                        pair_count       INTEGER,
                        top_pair         TEXT,
                        source           TEXT NOT NULL DEFAULT 'dexscreener',
                        snapshot_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_token_universe_mint "
                    "ON solana_token_universe(mint)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_token_universe_snapshot "
                    "ON solana_token_universe(snapshot_at)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_token_universe_mint_snap "
                    "ON solana_token_universe(mint, snapshot_at DESC)"
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS solana_mint_enrichment (
                        mint              TEXT PRIMARY KEY,
                        first_enriched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        last_enriched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        safety_passed     BOOLEAN,
                        safety_blockers   TEXT,
                        deployer          TEXT,
                        deployer_score    FLOAT,
                        error             TEXT
                    )
                    """
                )
            )

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------
    def ingest_once(self) -> IngestSummary:
        """Run one snapshot + optional enrichment pass.

        Returns an :class:`IngestSummary` — the method never raises on
        provider errors; the caller inspects the summary.
        """
        snapshot_at = self._clock()
        summary = IngestSummary(snapshot_at=snapshot_at)

        try:
            tokens = self.provider.list_top_by_volume(self.limit)
        except Exception as exc:  # noqa: BLE001 — batch-level isolation
            log.error("Provider failed during ingest: {e}", e=str(exc))
            summary.http_errors += 1
            return summary

        summary.tokens_considered = len(tokens)
        if not tokens:
            return summary

        # Inherit provider HTTP errors if the provider exposes a counter.
        provider_errors = getattr(self.provider, "http_errors", 0)
        if provider_errors:
            summary.http_errors += int(provider_errors)
            # Reset so we only attribute errors to this run.
            try:
                self.provider.http_errors = 0  # type: ignore[attr-defined]
            except Exception:
                pass

        # Write every token to the universe table first — enrichment is
        # secondary and must not delay the snapshot.
        self._write_snapshot(tokens, snapshot_at)
        summary.tokens_written = len(tokens)

        if self.enrich_on_insert and (self.safety or self.deployer_registry):
            self._enrich_new_mints(tokens, snapshot_at, summary)

        log.info(
            "TopVolumeIngestor.ingest_once: {n} tokens, {e} enriched, {er} errors",
            n=summary.tokens_written,
            e=summary.new_mints_enriched,
            er=summary.enrichment_errors,
        )
        return summary

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _write_snapshot(
        self,
        tokens: list[TokenVolumeSnapshot],
        snapshot_at: datetime,
    ) -> None:
        with self.engine.begin() as conn:
            for rank, token in enumerate(tokens, start=1):
                conn.execute(
                    text(
                        """
                        INSERT INTO solana_token_universe
                            (mint, symbol, name, rank, volume_24h_usd,
                             price_usd, market_cap_usd, liquidity_usd,
                             pair_count, top_pair, source, snapshot_at)
                        VALUES
                            (:mint, :sym, :name, :rank, :vol,
                             :price, :mc, :liq, :pc, :tp, :src, :snap)
                        """
                    ),
                    {
                        "mint": token.mint,
                        "sym": token.symbol,
                        "name": token.name,
                        "rank": rank,
                        "vol": token.volume_24h_usd,
                        "price": token.price_usd,
                        "mc": token.market_cap_usd,
                        "liq": token.liquidity_usd,
                        "pc": token.pair_count,
                        "tp": token.top_pair,
                        "src": token.source,
                        "snap": snapshot_at,
                    },
                )

    # ------------------------------------------------------------------
    # Enrichment
    # ------------------------------------------------------------------
    def _enrich_new_mints(
        self,
        tokens: list[TokenVolumeSnapshot],
        snapshot_at: datetime,
        summary: IngestSummary,
    ) -> None:
        known = self._load_known_mints([t.mint for t in tokens])
        new_tokens = [t for t in tokens if t.mint not in known]
        if not new_tokens:
            return

        log.info(
            "Enriching {n} new mints (of {total})",
            n=len(new_tokens), total=len(tokens),
        )

        for token in new_tokens:
            try:
                report = self._run_safety(token.mint)
                deployer = None
                deployer_score: float | None = None
                # Deployer resolution is best-effort — we don't block
                # the snapshot on it, and a provider failure just
                # leaves the row unresolved until next run.
                if self.deployer_registry is not None:
                    deployer, deployer_score = self._resolve_deployer(token.mint)

                self._upsert_enrichment(
                    mint=token.mint,
                    snapshot_at=snapshot_at,
                    report=report,
                    deployer=deployer,
                    deployer_score=deployer_score,
                )
                summary.new_mints_enriched += 1
            except Exception as exc:  # noqa: BLE001 — per-token isolation
                summary.enrichment_errors += 1
                log.warning(
                    "Enrichment failed for {m}: {e}",
                    m=token.mint, e=str(exc),
                )
                self._upsert_enrichment(
                    mint=token.mint,
                    snapshot_at=snapshot_at,
                    report=None,
                    deployer=None,
                    deployer_score=None,
                    error=str(exc)[:500],
                )

    def _run_safety(self, mint: str) -> TokenSafetyReport | None:
        if self.safety is None:
            return None
        return self.safety.check_token(mint)

    def _resolve_deployer(
        self, mint: str
    ) -> tuple[str | None, float | None]:
        """Best-effort deployer resolution via the configured provider.

        Walks:
          1. ``deploy_provider.get_mint_deployer(mint)`` — mint → wallet
          2. ``deployer_registry.refresh_wallet(wallet)`` — pulls the
             wallet's launch history, scores it, and persists the row

        Either step can fail without killing the enrichment — we log
        and return ``(None, None)`` so the caller persists an empty
        deployer row the current snapshot can still move on.
        """
        if self.deploy_provider is None:
            return (None, None)

        try:
            wallet = self.deploy_provider.get_mint_deployer(mint)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "get_mint_deployer failed for {m}: {e}",
                m=mint, e=str(exc),
            )
            return (None, None)

        if not wallet:
            return (None, None)

        if self.deployer_registry is None:
            return (wallet, None)

        try:
            result = self.deployer_registry.refresh_wallet(wallet)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "refresh_wallet failed for {w}: {e}",
                w=wallet, e=str(exc),
            )
            return (wallet, None)

        return (wallet, float(result.score))

    def _upsert_enrichment(
        self,
        *,
        mint: str,
        snapshot_at: datetime,
        report: TokenSafetyReport | None,
        deployer: str | None,
        deployer_score: float | None,
        error: str | None = None,
    ) -> None:
        safety_passed: bool | None = None
        blockers: str | None = None
        if report is not None:
            safety_passed = report.passed
            if report.blockers:
                blockers = ",".join(b.name for b in report.blockers)

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO solana_mint_enrichment
                        (mint, first_enriched_at, last_enriched_at,
                         safety_passed, safety_blockers,
                         deployer, deployer_score, error)
                    VALUES
                        (:mint, :snap, :snap, :sp, :sb, :d, :ds, :err)
                    ON CONFLICT (mint) DO UPDATE SET
                        last_enriched_at = EXCLUDED.last_enriched_at,
                        safety_passed = EXCLUDED.safety_passed,
                        safety_blockers = EXCLUDED.safety_blockers,
                        deployer = COALESCE(EXCLUDED.deployer, solana_mint_enrichment.deployer),
                        deployer_score = COALESCE(EXCLUDED.deployer_score, solana_mint_enrichment.deployer_score),
                        error = EXCLUDED.error
                    """
                ),
                {
                    "mint": mint,
                    "snap": snapshot_at,
                    "sp": safety_passed,
                    "sb": blockers,
                    "d": deployer,
                    "ds": deployer_score,
                    "err": error,
                },
            )

    def _load_known_mints(self, mints: list[str]) -> set[str]:
        if not mints:
            return set()
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT mint FROM solana_mint_enrichment "
                    "WHERE mint = ANY(:ms)"
                ),
                {"ms": mints},
            ).fetchall()
        return {r[0] for r in rows}


# ----------------------------------------------------------------------
# Internal helpers
# ----------------------------------------------------------------------
def _batched(items: Iterable[Any], size: int) -> Iterable[list[Any]]:
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


@dataclass
class _MintAccumulator:
    """Mutable accumulator used while folding DexScreener pairs.

    We track the single highest-volume pair separately so ``top_pair``
    is unambiguous even after the total volume has grown from merges.
    """

    mint: str
    symbol: str | None = None
    name: str | None = None
    volume_24h_usd: float = 0.0
    max_liquidity_usd: float | None = None
    top_pair_id: str | None = None
    top_pair_volume_usd: float = 0.0
    # Price/MC come from the top pair so the numbers match what the
    # operator would see on DexScreener's UI for the lead market.
    top_pair_price_usd: float | None = None
    top_pair_market_cap_usd: float | None = None
    pair_count: int = 0

    def add_pair(self, pair: dict[str, Any]) -> None:
        base = pair.get("baseToken") or {}
        volume = _safe_float((pair.get("volume") or {}).get("h24")) or 0.0
        liquidity = _safe_float((pair.get("liquidity") or {}).get("usd"))
        price = _safe_float(pair.get("priceUsd"))
        market_cap = _safe_float(pair.get("marketCap"))
        pair_id = pair.get("pairAddress") if isinstance(pair.get("pairAddress"), str) else None

        self.pair_count += 1
        self.volume_24h_usd += volume
        if self.symbol is None and isinstance(base.get("symbol"), str):
            self.symbol = base["symbol"]
        if self.name is None and isinstance(base.get("name"), str):
            self.name = base["name"]
        if liquidity is not None:
            current_max = self.max_liquidity_usd or 0.0
            if liquidity > current_max:
                self.max_liquidity_usd = liquidity

        if volume > self.top_pair_volume_usd:
            self.top_pair_volume_usd = volume
            self.top_pair_id = pair_id
            self.top_pair_price_usd = price
            self.top_pair_market_cap_usd = market_cap

    def to_snapshot(self) -> TokenVolumeSnapshot:
        return TokenVolumeSnapshot(
            mint=self.mint,
            symbol=self.symbol,
            name=self.name,
            volume_24h_usd=self.volume_24h_usd,
            price_usd=self.top_pair_price_usd,
            market_cap_usd=self.top_pair_market_cap_usd,
            liquidity_usd=self.max_liquidity_usd,
            pair_count=self.pair_count,
            top_pair=self.top_pair_id,
        )


def _fold_pair(
    pair: dict[str, Any],
    accum: dict[str, _MintAccumulator],
) -> None:
    """Fold one DexScreener pair row into the per-mint accumulator."""
    base = pair.get("baseToken") or {}
    mint = base.get("address")
    if not isinstance(mint, str) or not mint:
        return
    a = accum.get(mint)
    if a is None:
        a = _MintAccumulator(mint=mint)
        accum[mint] = a
    a.add_pair(pair)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
