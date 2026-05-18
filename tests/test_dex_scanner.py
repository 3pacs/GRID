"""Tests for DEX liquidity spike detection."""

from ingestion.realtime.feeds.dex_scanner import detect_spikes, PoolData


class TestDetectSpikes:
    def test_volume_spike_detected(self):
        pool = PoolData(
            symbol="SOL:BONK", chain="solana", dex="raydium", pool_address="abc123",
            price=0.00002, volume_24h=500_000.0, volume_avg_24h=100_000.0,
            liquidity=200_000.0, price_change_1h=5.0, pool_age_hours=48.0,
        )
        spikes = detect_spikes([pool])
        assert len(spikes) == 1
        # PR #185: direction column is reserved for {BULL,BEAR,NEUTRAL,NULL};
        # categorical spike type lives in signal_subtype.
        assert spikes[0]["direction"] is None
        assert spikes[0]["signal_subtype"] == "spike_volume"
        assert spikes[0]["magnitude"] == 5.0

    def test_no_spike_normal_volume(self):
        pool = PoolData(
            symbol="ETH:UNI", chain="ethereum", dex="uniswap_v3", pool_address="def456",
            price=7.5, volume_24h=200_000.0, volume_avg_24h=150_000.0,
            liquidity=1_000_000.0, price_change_1h=2.0, pool_age_hours=720.0,
        )
        spikes = detect_spikes([pool])
        assert len(spikes) == 0

    def test_new_pool_with_liquidity(self):
        pool = PoolData(
            symbol="SOL:NEWCOIN", chain="solana", dex="raydium", pool_address="ghi789",
            price=0.001, volume_24h=10_000.0, volume_avg_24h=0.0,
            liquidity=75_000.0, price_change_1h=0.0, pool_age_hours=2.0,
        )
        spikes = detect_spikes([pool])
        assert len(spikes) == 1
        assert spikes[0]["direction"] is None
        assert spikes[0]["signal_subtype"] == "new_pool"

    def test_price_surge(self):
        pool = PoolData(
            symbol="ETH:MEME", chain="ethereum", dex="uniswap_v3", pool_address="jkl012",
            price=0.05, volume_24h=50_000.0, volume_avg_24h=40_000.0,
            liquidity=100_000.0, price_change_1h=25.0, pool_age_hours=168.0,
        )
        spikes = detect_spikes([pool])
        assert len(spikes) == 1
        assert spikes[0]["direction"] is None
        assert spikes[0]["signal_subtype"] == "price_surge"

    def test_volume_spike_takes_priority_over_price_surge(self):
        pool = PoolData(
            symbol="SOL:HOT", chain="solana", dex="raydium", pool_address="xyz",
            price=1.0, volume_24h=400_000.0, volume_avg_24h=100_000.0,
            liquidity=500_000.0, price_change_1h=30.0, pool_age_hours=100.0,
        )
        spikes = detect_spikes([pool])
        assert len(spikes) == 1
        assert spikes[0]["direction"] is None
        assert spikes[0]["signal_subtype"] == "spike_volume"  # volume spike fires first due to continue

    def test_empty_pools(self):
        assert detect_spikes([]) == []
