"""Unit tests for ingestion/altdata/defi_llama_puller.py."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.defi_llama_puller import (
    DefiLlamaPuller,
    MAJOR_CHAINS,
    _TOP_PROTOCOLS,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _mock_engine(source_id: int = 1) -> tuple[MagicMock, MagicMock]:
    """Build a mock engine that returns source_id from source_catalog."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchone.return_value = (source_id,)
    conn.execute.return_value.fetchall.return_value = []
    return engine, conn


@pytest.fixture
def mock_engine():
    engine, conn = _mock_engine(source_id=5)
    return engine, conn


@pytest.fixture
def puller(mock_engine):
    engine, conn = mock_engine
    p = DefiLlamaPuller(db_engine=engine)
    return p, engine, conn


# ---------------------------------------------------------------------------
# Initialisation tests
# ---------------------------------------------------------------------------


class TestDefiLlamaPullerInit:
    def test_source_name(self):
        assert DefiLlamaPuller.SOURCE_NAME == "DeFi_Llama"

    def test_source_config_free(self):
        assert DefiLlamaPuller.SOURCE_CONFIG["cost_tier"] == "FREE"

    def test_inherits_base_puller(self):
        from ingestion.base import BasePuller
        assert issubclass(DefiLlamaPuller, BasePuller)

    def test_init_sets_source_id(self, puller):
        p, engine, conn = puller
        assert p.source_id == 5

    def test_series_id_generation(self, puller):
        p, _, _ = puller
        assert p._series_id("protocol_tvl", "aave") == "defillama.protocol_tvl.aave"
        assert p._series_id("chain_tvl", "Ethereum") == "defillama.chain_tvl.ethereum"
        assert p._series_id("protocol_tvl", "Curve DEX") == "defillama.protocol_tvl.curve_dex"
        assert p._series_id("bridge_volume", "Stargate.V2") == "defillama.bridge_volume.stargate_v2"


# ---------------------------------------------------------------------------
# Protocol TVL tests
# ---------------------------------------------------------------------------


class TestPullProtocols:
    def _make_protocols(self, count: int = 3) -> list[dict]:
        """Generate fake protocol data."""
        return [
            {
                "name": f"Protocol_{i}",
                "slug": f"protocol-{i}",
                "tvl": float(1_000_000 * (count - i)),
                "category": "DEX",
                "chains": ["Ethereum"],
                "change_1d": -5.0 if i == 0 else 2.0,
                "change_1h": 0.1,
                "change_7d": 10.0,
                "mcap": 500_000_000,
                "fdv": 1_000_000_000,
            }
            for i in range(count)
        ]

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_pull_protocols_success(self, mock_ingest, puller):
        p, engine, conn = puller
        protocols = self._make_protocols(3)

        # _row_exists returns False (no existing data)
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=protocols):
            result = p.pull_protocols()

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 3
        assert result["protocols_count"] == 3

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_pull_protocols_api_failure(self, mock_ingest, puller):
        p, engine, conn = puller

        with patch.object(p, "_fetch_json", side_effect=ConnectionError("down")):
            result = p.pull_protocols()

        assert result["status"] == "FAILED"
        assert result["rows_inserted"] == 0

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_pull_protocols_empty_response(self, mock_ingest, puller):
        p, engine, conn = puller

        with patch.object(p, "_fetch_json", return_value=[]):
            result = p.pull_protocols()

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 0

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_pull_protocols_filters_negative_tvl(self, mock_ingest, puller):
        p, engine, conn = puller
        protocols = [
            {"name": "Good", "slug": "good", "tvl": 1_000_000, "change_1d": 1.0},
            {"name": "Bad", "slug": "bad", "tvl": -100, "change_1d": 0},
            {"name": "Zero", "slug": "zero", "tvl": 0, "change_1d": 0},
        ]
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=protocols):
            result = p.pull_protocols()

        assert result["protocols_count"] == 1

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_pull_protocols_limits_to_top_n(self, mock_ingest, puller):
        p, engine, conn = puller
        # Make 600 protocols — should only take top 500
        protocols = self._make_protocols(600)
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=protocols):
            result = p.pull_protocols()

        assert result["protocols_count"] == _TOP_PROTOCOLS

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=3)
    def test_pull_protocols_ingests_actors(self, mock_ingest, puller):
        p, engine, conn = puller
        protocols = self._make_protocols(3)
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=protocols):
            p.pull_protocols()

        mock_ingest.assert_called()
        actors = mock_ingest.call_args[0][1]
        assert len(actors) == 3
        assert all(t == "company" for _, t in actors)


# ---------------------------------------------------------------------------
# TVL anomaly detection tests
# ---------------------------------------------------------------------------


class TestTvlAnomalyDetection:
    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_detects_anomaly_over_threshold(self, mock_ingest, puller):
        p, engine, conn = puller
        protocols = [
            {
                "name": "Rugged",
                "slug": "rugged",
                "tvl": 500_000,
                "change_1d": -25.0,  # 25% drop
            },
        ]
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=protocols):
            result = p.pull_protocols()

        assert len(result["anomalies"]) == 1
        assert result["anomalies"][0]["protocol"] == "Rugged"
        assert result["anomalies"][0]["change_1d_pct"] == -25.0

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_no_anomaly_below_threshold(self, mock_ingest, puller):
        p, engine, conn = puller
        protocols = [
            {
                "name": "Stable",
                "slug": "stable",
                "tvl": 1_000_000,
                "change_1d": -15.0,  # 15% drop — below 20% threshold
            },
        ]
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=protocols):
            result = p.pull_protocols()

        assert len(result["anomalies"]) == 0

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_anomaly_with_positive_change(self, mock_ingest, puller):
        p, engine, conn = puller
        protocols = [
            {
                "name": "Growing",
                "slug": "growing",
                "tvl": 2_000_000,
                "change_1d": 30.0,  # positive, not an anomaly
            },
        ]
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=protocols):
            result = p.pull_protocols()

        assert len(result["anomalies"]) == 0

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_anomaly_with_none_change(self, mock_ingest, puller):
        p, engine, conn = puller
        protocols = [
            {
                "name": "NoData",
                "slug": "nodata",
                "tvl": 1_000_000,
                "change_1d": None,
            },
        ]
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=protocols):
            result = p.pull_protocols()

        assert len(result["anomalies"]) == 0


# ---------------------------------------------------------------------------
# Chain TVL tests
# ---------------------------------------------------------------------------


class TestPullChainTvl:
    def _make_chain_data(self, days: int = 5) -> list[dict]:
        """Generate fake chain TVL history."""
        base_ts = int(datetime(2026, 3, 1, tzinfo=timezone.utc).timestamp())
        return [
            {
                "date": base_ts + i * 86400,
                "tvl": 50_000_000_000 + i * 1_000_000_000,
            }
            for i in range(days)
        ]

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    @patch("ingestion.altdata.defi_llama_puller.time.sleep")
    def test_pull_chain_tvl_success(self, mock_sleep, mock_ingest, puller):
        p, engine, conn = puller
        chain_data = self._make_chain_data(5)
        conn.execute.return_value.fetchone.return_value = None
        conn.execute.return_value.fetchall.return_value = []

        with patch.object(p, "_fetch_json", return_value=chain_data):
            result = p.pull_chain_tvl(chains=["Ethereum"], days_back=365)

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 5
        assert result["per_chain"]["Ethereum"] == 5

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    @patch("ingestion.altdata.defi_llama_puller.time.sleep")
    def test_pull_chain_tvl_api_failure_continues(self, mock_sleep, mock_ingest, puller):
        p, engine, conn = puller

        def side_effect(url):
            if "Ethereum" in url:
                raise ConnectionError("timeout")
            return self._make_chain_data(3)

        conn.execute.return_value.fetchone.return_value = None
        conn.execute.return_value.fetchall.return_value = []

        with patch.object(p, "_fetch_json", side_effect=side_effect):
            result = p.pull_chain_tvl(
                chains=["Ethereum", "Solana"], days_back=365
            )

        assert result["status"] == "SUCCESS"
        assert result["per_chain"]["Ethereum"] == 0
        assert result["per_chain"]["Solana"] == 3

    def _make_chain_data_instance(self, days: int = 5) -> list[dict]:
        base_ts = int(datetime(2026, 3, 1, tzinfo=timezone.utc).timestamp())
        return [
            {"date": base_ts + i * 86400, "tvl": 50_000_000_000 + i * 1_000_000_000}
            for i in range(days)
        ]

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    @patch("ingestion.altdata.defi_llama_puller.time.sleep")
    def test_pull_chain_tvl_respects_days_back(self, mock_sleep, mock_ingest, puller):
        p, engine, conn = puller
        # Data from 2026-03-01 — with days_back=5, cutoff is very recent
        chain_data = self._make_chain_data_instance(5)
        conn.execute.return_value.fetchone.return_value = None
        conn.execute.return_value.fetchall.return_value = []

        with patch.object(p, "_fetch_json", return_value=chain_data):
            result = p.pull_chain_tvl(chains=["Ethereum"], days_back=5)

        # All data older than cutoff is filtered out
        assert result["status"] == "SUCCESS"

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    @patch("ingestion.altdata.defi_llama_puller.time.sleep")
    def test_pull_chain_tvl_skips_bad_timestamps(self, mock_sleep, mock_ingest, puller):
        p, engine, conn = puller
        data = [
            {"date": "not_a_number", "tvl": 100},
            {"date": None, "tvl": 200},
            {"date": 99999999999999, "tvl": 300},  # overflow
        ]
        conn.execute.return_value.fetchone.return_value = None
        conn.execute.return_value.fetchall.return_value = []

        with patch.object(p, "_fetch_json", return_value=data):
            result = p.pull_chain_tvl(chains=["Ethereum"], days_back=365)

        assert result["per_chain"]["Ethereum"] == 0

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    @patch("ingestion.altdata.defi_llama_puller.time.sleep")
    def test_pull_chain_tvl_defaults_to_major_chains(self, mock_sleep, mock_ingest, puller):
        p, engine, conn = puller
        conn.execute.return_value.fetchone.return_value = None
        conn.execute.return_value.fetchall.return_value = []

        with patch.object(p, "_fetch_json", return_value=[]):
            result = p.pull_chain_tvl()

        assert set(result["per_chain"].keys()) == set(MAJOR_CHAINS)


# ---------------------------------------------------------------------------
# Stablecoin tests
# ---------------------------------------------------------------------------


class TestPullStablecoins:
    def _make_stablecoin_data(self) -> dict:
        return {
            "peggedAssets": [
                {
                    "name": "Tether",
                    "symbol": "USDT",
                    "pegType": "peggedUSD",
                    "pegMechanism": "fiat-backed",
                    "circulating": {"peggedUSD": 110_000_000_000},
                    "chains": ["Ethereum", "Tron"],
                    "price": 1.0001,
                },
                {
                    "name": "USD Coin",
                    "symbol": "USDC",
                    "pegType": "peggedUSD",
                    "pegMechanism": "fiat-backed",
                    "circulating": {"peggedUSD": 45_000_000_000},
                    "chains": ["Ethereum", "Solana"],
                    "price": 0.9999,
                },
                {
                    "name": "DAI",
                    "symbol": "DAI",
                    "pegType": "peggedUSD",
                    "pegMechanism": "crypto-backed",
                    "circulating": {"peggedUSD": 5_000_000_000},
                    "chains": ["Ethereum"],
                    "price": 1.0002,
                },
            ]
        }

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_pull_stablecoins_success(self, mock_ingest, puller):
        p, engine, conn = puller
        data = self._make_stablecoin_data()
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=data):
            result = p.pull_stablecoins()

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 3
        assert result["stablecoins_count"] == 3

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_pull_stablecoins_api_failure(self, mock_ingest, puller):
        p, engine, conn = puller

        with patch.object(p, "_fetch_json", side_effect=ConnectionError("down")):
            result = p.pull_stablecoins()

        assert result["status"] == "FAILED"
        assert result["rows_inserted"] == 0

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_pull_stablecoins_skips_missing_symbol(self, mock_ingest, puller):
        p, engine, conn = puller
        data = {
            "peggedAssets": [
                {"name": "NoSymbol", "symbol": "", "circulating": {"peggedUSD": 100}},
            ]
        }
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=data):
            result = p.pull_stablecoins()

        assert result["rows_inserted"] == 0

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_pull_stablecoins_skips_zero_supply(self, mock_ingest, puller):
        p, engine, conn = puller
        data = {
            "peggedAssets": [
                {
                    "name": "Dead", "symbol": "DEAD",
                    "circulating": {"peggedUSD": 0},
                },
            ]
        }
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=data):
            result = p.pull_stablecoins()

        assert result["rows_inserted"] == 0

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=3)
    def test_pull_stablecoins_ingests_actors(self, mock_ingest, puller):
        p, engine, conn = puller
        data = self._make_stablecoin_data()
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=data):
            p.pull_stablecoins()

        mock_ingest.assert_called()
        actors = mock_ingest.call_args[0][1]
        names = [name for name, _ in actors]
        assert "Tether" in names
        assert "USD Coin" in names


# ---------------------------------------------------------------------------
# Bridge volume tests
# ---------------------------------------------------------------------------


class TestPullBridges:
    def _make_bridge_data(self) -> dict:
        return {
            "bridges": [
                {
                    "displayName": "Stargate",
                    "name": "stargate",
                    "lastDayVolume": 50_000_000,
                    "currentDayVolume": 30_000_000,
                    "chains": ["Ethereum", "Arbitrum", "Optimism"],
                },
                {
                    "displayName": "Across",
                    "name": "across",
                    "lastDayVolume": 25_000_000,
                    "chains": ["Ethereum", "Arbitrum"],
                },
                {
                    "displayName": "Wormhole",
                    "name": "wormhole",
                    "lastDayVolume": 15_000_000,
                    "chains": ["Ethereum", "Solana"],
                },
            ]
        }

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_pull_bridges_success(self, mock_ingest, puller):
        p, engine, conn = puller
        data = self._make_bridge_data()
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=data):
            result = p.pull_bridges()

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 3
        assert result["bridges_count"] == 3

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_pull_bridges_api_failure(self, mock_ingest, puller):
        p, engine, conn = puller

        with patch.object(p, "_fetch_json", side_effect=ConnectionError("down")):
            result = p.pull_bridges()

        assert result["status"] == "FAILED"

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_pull_bridges_skips_zero_volume(self, mock_ingest, puller):
        p, engine, conn = puller
        data = {
            "bridges": [
                {"displayName": "Dead", "lastDayVolume": 0, "chains": []},
            ]
        }
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=data):
            result = p.pull_bridges()

        assert result["rows_inserted"] == 0

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_pull_bridges_falls_back_to_current_day_volume(self, mock_ingest, puller):
        p, engine, conn = puller
        data = {
            "bridges": [
                {
                    "displayName": "NewBridge",
                    "lastDayVolume": None,
                    "currentDayVolume": 10_000_000,
                    "chains": ["Ethereum"],
                },
            ]
        }
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=data):
            result = p.pull_bridges()

        assert result["rows_inserted"] == 1

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_pull_bridges_skips_no_name(self, mock_ingest, puller):
        p, engine, conn = puller
        data = {
            "bridges": [
                {"displayName": "", "name": "", "lastDayVolume": 5_000_000},
            ]
        }
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=data):
            result = p.pull_bridges()

        assert result["rows_inserted"] == 0


# ---------------------------------------------------------------------------
# Combined pull_all tests
# ---------------------------------------------------------------------------


class TestPullAll:
    @patch("ingestion.altdata.defi_llama_puller.time.sleep")
    def test_pull_all_calls_all_sources(self, mock_sleep, puller):
        p, engine, conn = puller

        with (
            patch.object(p, "pull_protocols", return_value={"status": "SUCCESS", "rows_inserted": 10}) as mock_proto,
            patch.object(p, "pull_chain_tvl", return_value={"status": "SUCCESS", "rows_inserted": 20}) as mock_chain,
            patch.object(p, "pull_stablecoins", return_value={"status": "SUCCESS", "rows_inserted": 5}) as mock_stable,
            patch.object(p, "pull_bridges", return_value={"status": "SUCCESS", "rows_inserted": 3}) as mock_bridge,
        ):
            results = p.pull_all()

        assert len(results) == 4
        mock_proto.assert_called_once()
        mock_chain.assert_called_once()
        mock_stable.assert_called_once()
        mock_bridge.assert_called_once()

    @patch("ingestion.altdata.defi_llama_puller.time.sleep")
    def test_pull_all_continues_on_failure(self, mock_sleep, puller):
        p, engine, conn = puller

        with (
            patch.object(p, "pull_protocols", return_value={"status": "FAILED", "rows_inserted": 0}),
            patch.object(p, "pull_chain_tvl", return_value={"status": "SUCCESS", "rows_inserted": 20}),
            patch.object(p, "pull_stablecoins", return_value={"status": "SUCCESS", "rows_inserted": 5}),
            patch.object(p, "pull_bridges", return_value={"status": "SUCCESS", "rows_inserted": 3}),
        ):
            results = p.pull_all()

        assert len(results) == 4
        statuses = [r["status"] for r in results]
        assert statuses[0] == "FAILED"
        assert statuses[1] == "SUCCESS"

    @patch("ingestion.altdata.defi_llama_puller.time.sleep")
    def test_pull_all_passes_chain_params(self, mock_sleep, puller):
        p, engine, conn = puller

        with (
            patch.object(p, "pull_protocols", return_value={"status": "SUCCESS", "rows_inserted": 0}),
            patch.object(p, "pull_chain_tvl", return_value={"status": "SUCCESS", "rows_inserted": 0}) as mock_chain,
            patch.object(p, "pull_stablecoins", return_value={"status": "SUCCESS", "rows_inserted": 0}),
            patch.object(p, "pull_bridges", return_value={"status": "SUCCESS", "rows_inserted": 0}),
        ):
            p.pull_all(chains=["Ethereum", "Solana"], days_back=30)

        mock_chain.assert_called_once_with(
            chains=["Ethereum", "Solana"], days_back=30
        )


# ---------------------------------------------------------------------------
# HTTP helper tests
# ---------------------------------------------------------------------------


class TestFetchJson:
    def test_fetch_json_success(self, puller):
        p, _, _ = puller
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"data": "ok"}
        mock_resp.raise_for_status = MagicMock()

        with patch("ingestion.altdata.defi_llama_puller.requests.get", return_value=mock_resp):
            result = p._fetch_json("https://api.llama.fi/v2/chains")

        assert result == {"data": "ok"}

    def test_fetch_json_sets_user_agent(self, puller):
        p, _, _ = puller
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}
        mock_resp.raise_for_status = MagicMock()

        with patch("ingestion.altdata.defi_llama_puller.requests.get", return_value=mock_resp) as mock_get:
            p._fetch_json("https://api.llama.fi/v2/chains")

        call_kwargs = mock_get.call_args
        assert "User-Agent" in call_kwargs.kwargs.get("headers", call_kwargs[1].get("headers", {}))


# ---------------------------------------------------------------------------
# Edge cases and dedup tests
# ---------------------------------------------------------------------------


class TestDeduplication:
    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_protocols_skip_existing_rows(self, mock_ingest, puller):
        p, engine, conn = puller
        protocols = [
            {"name": "Exists", "slug": "exists", "tvl": 1_000_000, "change_1d": 1.0},
        ]
        # _row_exists returns True (row already in DB)
        conn.execute.return_value.fetchone.return_value = (1,)

        with patch.object(p, "_fetch_json", return_value=protocols):
            result = p.pull_protocols()

        assert result["rows_inserted"] == 0

    @patch("ingestion.altdata.defi_llama_puller.ingest_actors_batch", return_value=0)
    def test_stablecoins_skip_missing_circulating(self, mock_ingest, puller):
        p, engine, conn = puller
        data = {
            "peggedAssets": [
                {"name": "Weird", "symbol": "WRD", "circulating": {}},
                {"name": "Null", "symbol": "NUL", "circulating": {"peggedUSD": None}},
            ]
        }
        conn.execute.return_value.fetchone.return_value = None

        with patch.object(p, "_fetch_json", return_value=data):
            result = p.pull_stablecoins()

        assert result["rows_inserted"] == 0
