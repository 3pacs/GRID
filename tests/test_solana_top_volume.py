"""
Tests for ingestion/solana/top_volume.py.

All HTTP and DB traffic is mocked.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import httpx
import pytest
from sqlalchemy.engine import Engine

from ingestion.solana.top_volume import (
    JupiterDexScreenerProvider,
    TokenVolumeSnapshot,
    TopVolumeIngestor,
    _batched,
    _fold_pair,
)


NOW = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)


# ----------------------------------------------------------------------
# _batched
# ----------------------------------------------------------------------
def test_batched_even():
    assert list(_batched([1, 2, 3, 4], 2)) == [[1, 2], [3, 4]]


def test_batched_uneven():
    assert list(_batched([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]


def test_batched_empty():
    assert list(_batched([], 3)) == []


# ----------------------------------------------------------------------
# _MintAccumulator / _fold_pair
# ----------------------------------------------------------------------
def _pair(
    mint: str = "MINT1",
    symbol: str = "SYM",
    name: str = "Name",
    volume: float = 100_000.0,
    liquidity: float = 500_000.0,
    price: float = 1.5,
    market_cap: float = 2_000_000.0,
    pair_address: str = "POOL1",
) -> dict:
    return {
        "baseToken": {"address": mint, "symbol": symbol, "name": name},
        "volume": {"h24": volume},
        "liquidity": {"usd": liquidity},
        "priceUsd": price,
        "marketCap": market_cap,
        "pairAddress": pair_address,
    }


def test_fold_pair_single():
    accum: dict = {}
    _fold_pair(_pair(), accum)
    snap = accum["MINT1"].to_snapshot()
    assert snap.mint == "MINT1"
    assert snap.volume_24h_usd == 100_000.0
    assert snap.liquidity_usd == 500_000.0
    assert snap.top_pair == "POOL1"
    assert snap.pair_count == 1


def test_fold_pair_aggregates_volume_across_pairs():
    accum: dict = {}
    _fold_pair(_pair(volume=100_000, pair_address="P1"), accum)
    _fold_pair(_pair(volume=50_000, liquidity=200_000, pair_address="P2"), accum)
    snap = accum["MINT1"].to_snapshot()
    assert snap.volume_24h_usd == 150_000.0
    assert snap.pair_count == 2
    # Top pair is the higher-volume one
    assert snap.top_pair == "P1"
    # Max liquidity wins
    assert snap.liquidity_usd == 500_000.0


def test_fold_pair_top_pair_switches_on_larger_volume():
    accum: dict = {}
    _fold_pair(_pair(volume=50_000, pair_address="P1", price=1.0), accum)
    _fold_pair(_pair(volume=200_000, pair_address="P2", price=2.0), accum)
    snap = accum["MINT1"].to_snapshot()
    assert snap.top_pair == "P2"
    assert snap.price_usd == 2.0


def test_fold_pair_skips_invalid_mint():
    accum: dict = {}
    _fold_pair({"baseToken": {}}, accum)
    _fold_pair({}, accum)
    assert accum == {}


def test_fold_pair_handles_missing_fields():
    accum: dict = {}
    _fold_pair(
        {"baseToken": {"address": "M1"}},  # no volume, liquidity, price
        accum,
    )
    snap = accum["M1"].to_snapshot()
    assert snap.volume_24h_usd == 0.0
    assert snap.liquidity_usd is None
    assert snap.price_usd is None


# ----------------------------------------------------------------------
# JupiterDexScreenerProvider
# ----------------------------------------------------------------------
def _resp(json_data, status=200):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


@pytest.fixture()
def mock_http():
    return MagicMock(spec=httpx.Client)


def test_provider_fetches_jupiter_then_dex(mock_http):
    # First GET → Jupiter strict list
    # Subsequent GETs → DexScreener batches
    jup_response = _resp([
        {"address": f"MINT{i}", "symbol": f"S{i}", "name": f"Name{i}"}
        for i in range(3)
    ])
    dex_response = _resp({
        "pairs": [
            _pair(mint="MINT0", volume=300_000),
            _pair(mint="MINT1", volume=500_000),
            _pair(mint="MINT2", volume=100_000),
        ]
    })

    mock_http.get.side_effect = [jup_response, dex_response]
    provider = JupiterDexScreenerProvider(client=mock_http, request_delay=0)

    snapshots = provider.list_top_by_volume(limit=10)
    assert [s.mint for s in snapshots] == ["MINT1", "MINT0", "MINT2"]
    assert snapshots[0].volume_24h_usd == 500_000.0


def test_provider_respects_limit(mock_http):
    jup = _resp([{"address": f"M{i}"} for i in range(5)])
    dex = _resp({
        "pairs": [
            _pair(mint=f"M{i}", volume=1000 * (i + 1))
            for i in range(5)
        ]
    })
    mock_http.get.side_effect = [jup, dex]
    provider = JupiterDexScreenerProvider(client=mock_http, request_delay=0)

    snapshots = provider.list_top_by_volume(limit=2)
    assert len(snapshots) == 2
    assert snapshots[0].mint == "M4"
    assert snapshots[1].mint == "M3"


def test_provider_batches_dex_calls(mock_http):
    addresses = [{"address": f"M{i}"} for i in range(65)]
    jup = _resp(addresses)
    # 65 tokens / 30 per batch = 3 batches
    dex_batch_responses = [
        _resp({"pairs": [_pair(mint=f"M{i}", volume=1000) for i in range(30)]}),
        _resp({"pairs": [_pair(mint=f"M{i}", volume=1000) for i in range(30, 60)]}),
        _resp({"pairs": [_pair(mint=f"M{i}", volume=1000) for i in range(60, 65)]}),
    ]
    mock_http.get.side_effect = [jup, *dex_batch_responses]
    provider = JupiterDexScreenerProvider(
        client=mock_http, batch_size=30, request_delay=0
    )

    snapshots = provider.list_top_by_volume(limit=100)
    assert len(snapshots) == 65
    # Jupiter (1) + DexScreener (3)
    assert mock_http.get.call_count == 4


def test_provider_empty_jupiter_short_circuits(mock_http):
    mock_http.get.return_value = _resp([])
    provider = JupiterDexScreenerProvider(client=mock_http, request_delay=0)
    assert provider.list_top_by_volume(limit=10) == []
    # Only one call — no DexScreener queries at all.
    assert mock_http.get.call_count == 1


def test_provider_handles_jupiter_http_error(mock_http):
    mock_http.get.side_effect = httpx.ConnectError("down")
    provider = JupiterDexScreenerProvider(client=mock_http, request_delay=0)
    assert provider.list_top_by_volume(limit=10) == []
    assert provider.http_errors == 1


def test_provider_skips_dex_batch_on_error(mock_http):
    jup = _resp([{"address": f"M{i}"} for i in range(2)])
    mock_http.get.side_effect = [jup, httpx.ConnectError("boom")]
    provider = JupiterDexScreenerProvider(client=mock_http, request_delay=0)
    snapshots = provider.list_top_by_volume(limit=10)
    assert snapshots == []
    assert provider.http_errors == 1


def test_provider_handles_malformed_dex_response(mock_http):
    jup = _resp([{"address": "M1"}])
    # Missing "pairs" key
    dex = _resp({"unexpected": "shape"})
    mock_http.get.side_effect = [jup, dex]
    provider = JupiterDexScreenerProvider(client=mock_http, request_delay=0)
    snapshots = provider.list_top_by_volume(limit=10)
    assert snapshots == []


def test_provider_context_manager_closes_owned_client():
    p = JupiterDexScreenerProvider(request_delay=0)
    with p:
        pass
    # No crash; idempotent close
    p.close()


# ----------------------------------------------------------------------
# TopVolumeIngestor
# ----------------------------------------------------------------------
def _scripted_engine():
    engine = MagicMock(spec=Engine)
    conn = MagicMock()
    conn.execute.return_value = MagicMock()
    conn.execute.return_value.fetchone.return_value = None
    conn.execute.return_value.fetchall.return_value = []
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def _snap(mint: str, volume: float = 1_000_000) -> TokenVolumeSnapshot:
    return TokenVolumeSnapshot(
        mint=mint,
        symbol="SYM",
        name="Name",
        volume_24h_usd=volume,
        price_usd=1.0,
        market_cap_usd=10_000_000,
        liquidity_usd=500_000,
        pair_count=1,
        top_pair="POOL1",
    )


def test_ingestor_creates_tables_on_construct():
    engine, conn = _scripted_engine()
    provider = MagicMock()
    TopVolumeIngestor(engine, provider=provider)
    joined = " ".join(str(c.args[0]) for c in conn.execute.call_args_list).lower()
    assert "solana_token_universe" in joined
    assert "solana_mint_enrichment" in joined
    assert "idx_token_universe_mint" in joined


def test_ingestor_writes_snapshot_rows():
    engine, conn = _scripted_engine()
    provider = MagicMock()
    provider.list_top_by_volume.return_value = [
        _snap("M1", 500_000),
        _snap("M2", 300_000),
        _snap("M3", 100_000),
    ]
    ingestor = TopVolumeIngestor(
        engine, provider=provider, enrich_on_insert=False, limit=10,
        clock=lambda: NOW,
    )
    conn.execute.reset_mock()
    conn.execute.side_effect = None

    summary = ingestor.ingest_once()
    assert summary.tokens_considered == 3
    assert summary.tokens_written == 3
    # Three INSERT calls into solana_token_universe
    inserts = [
        c for c in conn.execute.call_args_list
        if "solana_token_universe" in str(c.args[0]).lower()
        and "insert" in str(c.args[0]).lower()
    ]
    assert len(inserts) == 3
    # Rank is 1-indexed
    first_bind = inserts[0].args[1]
    assert first_bind["rank"] == 1
    assert first_bind["mint"] == "M1"


def test_ingestor_enriches_only_new_mints():
    engine, conn = _scripted_engine()
    provider = MagicMock()
    provider.list_top_by_volume.return_value = [_snap("M1"), _snap("M2")]
    safety = MagicMock()

    from trading.solana.safety import SafetyCheck, TokenSafetyReport
    safety.check_token.return_value = TokenSafetyReport(
        mint="M1",
        checks=(SafetyCheck(name="ok", passed=True, severity="info", detail="x"),),
        passed=True,
    )

    ingestor = TopVolumeIngestor(
        engine, provider=provider, safety=safety,
        clock=lambda: NOW, limit=10,
    )
    conn.execute.reset_mock()
    conn.execute.side_effect = None

    # First SELECT against solana_mint_enrichment returns M1 as known,
    # M2 as new. All subsequent execute calls return a default mock.
    known_result = MagicMock()
    known_result.fetchall.return_value = [("M1",)]
    default_result = MagicMock()
    default_result.fetchone.return_value = None
    default_result.fetchall.return_value = []

    def _execute_router(*args, **kwargs):
        sql = str(args[0]).lower()
        if "select mint from solana_mint_enrichment" in sql:
            return known_result
        return default_result

    conn.execute.side_effect = _execute_router

    summary = ingestor.ingest_once()
    # Only M2 was new → one safety check
    assert safety.check_token.call_count == 1
    safety.check_token.assert_called_with("M2")
    assert summary.new_mints_enriched == 1
    assert summary.enrichment_errors == 0


def test_ingestor_provider_failure_returns_empty_summary():
    engine, conn = _scripted_engine()
    provider = MagicMock()
    provider.list_top_by_volume.side_effect = RuntimeError("down")

    ingestor = TopVolumeIngestor(engine, provider=provider, enrich_on_insert=False)
    summary = ingestor.ingest_once()
    assert summary.tokens_considered == 0
    assert summary.tokens_written == 0
    assert summary.http_errors == 1


def test_ingestor_enrichment_error_is_isolated():
    engine, conn = _scripted_engine()
    provider = MagicMock()
    provider.list_top_by_volume.return_value = [_snap("M1"), _snap("M2")]
    safety = MagicMock()
    # First call fine, second crashes
    from trading.solana.safety import SafetyCheck, TokenSafetyReport
    ok = TokenSafetyReport(
        mint="M1",
        checks=(SafetyCheck(name="ok", passed=True, severity="info", detail="x"),),
        passed=True,
    )
    safety.check_token.side_effect = [ok, RuntimeError("RPC down")]

    ingestor = TopVolumeIngestor(
        engine, provider=provider, safety=safety, clock=lambda: NOW,
    )
    conn.execute.reset_mock()
    conn.execute.side_effect = None
    default_result = MagicMock()
    default_result.fetchone.return_value = None
    default_result.fetchall.return_value = []
    conn.execute.return_value = default_result

    summary = ingestor.ingest_once()
    # One success, one error — both mints still processed
    assert summary.new_mints_enriched == 1
    assert summary.enrichment_errors == 1
    assert safety.check_token.call_count == 2


def test_ingestor_inherits_provider_http_errors():
    engine, conn = _scripted_engine()
    provider = MagicMock()
    provider.list_top_by_volume.return_value = [_snap("M1")]
    provider.http_errors = 3  # pre-existing errors from provider

    ingestor = TopVolumeIngestor(engine, provider=provider, enrich_on_insert=False)
    summary = ingestor.ingest_once()
    assert summary.http_errors == 3
    # Provider counter is reset so the next run attributes fresh errors
    assert provider.http_errors == 0


def test_ingestor_enrich_on_insert_false_skips_enrichment():
    engine, conn = _scripted_engine()
    provider = MagicMock()
    provider.list_top_by_volume.return_value = [_snap("M1")]
    safety = MagicMock()

    ingestor = TopVolumeIngestor(
        engine, provider=provider, safety=safety, enrich_on_insert=False,
    )
    ingestor.ingest_once()
    safety.check_token.assert_not_called()


# ----------------------------------------------------------------------
# Deployer resolution during enrichment
# ----------------------------------------------------------------------
def _setup_enrichment(
    provider_snapshots: list[TokenVolumeSnapshot],
    known_mints: list[str] | None = None,
):
    """Build (engine, conn, provider, safety) wired for enrichment tests."""
    engine, conn = _scripted_engine()
    provider = MagicMock()
    provider.list_top_by_volume.return_value = provider_snapshots
    safety = MagicMock()
    from trading.solana.safety import SafetyCheck, TokenSafetyReport
    safety.check_token.return_value = TokenSafetyReport(
        mint="M1",
        checks=(SafetyCheck(name="ok", passed=True, severity="info", detail="x"),),
        passed=True,
    )

    known = list(known_mints or [])
    default = MagicMock()
    default.fetchone.return_value = None
    default.fetchall.return_value = []

    known_result = MagicMock()
    known_result.fetchall.return_value = [(m,) for m in known]

    def _router(*args, **kwargs):
        sql = str(args[0]).lower()
        if "select mint from solana_mint_enrichment" in sql:
            return known_result
        return default

    conn.execute.reset_mock()
    conn.execute.side_effect = _router
    return engine, conn, provider, safety


def test_ingestor_resolves_deployer_and_scores_registry():
    engine, conn, provider, safety = _setup_enrichment(
        [_snap("M1")], known_mints=[]
    )
    deploy_provider = MagicMock()
    deploy_provider.get_mint_deployer.return_value = "DEPLOYER1"

    from trading.solana.deployer_registry import DeployerScoreResult, DeployerStats
    deployer_registry = MagicMock()
    deployer_registry.refresh_wallet.return_value = DeployerScoreResult(
        wallet="DEPLOYER1",
        score=0.72,
        components={},
        stats=DeployerStats("DEPLOYER1", 6, 4, 300_000, 800_000, 900, NOW),
        reasons=(),
    )

    ingestor = TopVolumeIngestor(
        engine,
        provider=provider,
        safety=safety,
        deploy_provider=deploy_provider,
        deployer_registry=deployer_registry,
        clock=lambda: NOW,
    )
    summary = ingestor.ingest_once()

    deploy_provider.get_mint_deployer.assert_called_once_with("M1")
    deployer_registry.refresh_wallet.assert_called_once_with("DEPLOYER1")
    assert summary.new_mints_enriched == 1


def test_ingestor_deployer_resolution_missing_provider_is_noop():
    engine, conn, provider, safety = _setup_enrichment(
        [_snap("M1")], known_mints=[]
    )
    ingestor = TopVolumeIngestor(
        engine, provider=provider, safety=safety,
        deploy_provider=None,
        clock=lambda: NOW,
    )
    summary = ingestor.ingest_once()
    assert summary.new_mints_enriched == 1
    # No crash — safety ran, deployer was simply skipped.


def test_ingestor_deployer_provider_error_is_isolated():
    engine, conn, provider, safety = _setup_enrichment(
        [_snap("M1")], known_mints=[]
    )
    deploy_provider = MagicMock()
    deploy_provider.get_mint_deployer.side_effect = RuntimeError("rpc down")
    deployer_registry = MagicMock()

    ingestor = TopVolumeIngestor(
        engine, provider=provider, safety=safety,
        deploy_provider=deploy_provider,
        deployer_registry=deployer_registry,
        clock=lambda: NOW,
    )
    summary = ingestor.ingest_once()
    # Enrichment still counted as successful — deployer is best-effort.
    assert summary.new_mints_enriched == 1
    assert summary.enrichment_errors == 0
    deployer_registry.refresh_wallet.assert_not_called()


def test_ingestor_deployer_refresh_error_keeps_wallet():
    engine, conn, provider, safety = _setup_enrichment(
        [_snap("M1")], known_mints=[]
    )
    deploy_provider = MagicMock()
    deploy_provider.get_mint_deployer.return_value = "DEPLOYER1"
    deployer_registry = MagicMock()
    deployer_registry.refresh_wallet.side_effect = RuntimeError("history empty")

    ingestor = TopVolumeIngestor(
        engine, provider=provider, safety=safety,
        deploy_provider=deploy_provider,
        deployer_registry=deployer_registry,
        clock=lambda: NOW,
    )
    summary = ingestor.ingest_once()
    assert summary.new_mints_enriched == 1
    deployer_registry.refresh_wallet.assert_called_once()
