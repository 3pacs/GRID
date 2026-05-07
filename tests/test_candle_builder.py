"""Tests for the in-memory OHLCV candle aggregator."""

from datetime import datetime, timezone

import pytest

from ingestion.realtime.candle_builder import CandleBuilder, CandleState


def make_ts(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def builder() -> CandleBuilder:
    return CandleBuilder()


# Bucket boundaries for a 5-minute interval (300s)
# 1000 * 300 = 300000 → bucket boundary at epoch=300000
BUCKET_BOUNDARY = 300 * 1000       # 300000 — exact boundary
MID_BUCKET = BUCKET_BOUNDARY + 90  # 90s into the bucket
END_BUCKET = BUCKET_BOUNDARY + 299 # last second before rollover
NEXT_BUCKET = BUCKET_BOUNDARY + 300  # first second of next bucket


# ---------------------------------------------------------------------------
# 1. Bucket floor
# ---------------------------------------------------------------------------

class TestBucketFloor:
    def test_exact_boundary(self, builder):
        ts = make_ts(BUCKET_BOUNDARY)
        bucket = builder._bucket_floor(ts)
        assert int(bucket.timestamp()) == BUCKET_BOUNDARY

    def test_mid_bucket_floors_down(self, builder):
        ts = make_ts(MID_BUCKET)
        bucket = builder._bucket_floor(ts)
        assert int(bucket.timestamp()) == BUCKET_BOUNDARY

    def test_end_of_bucket_floors_down(self, builder):
        ts = make_ts(END_BUCKET)
        bucket = builder._bucket_floor(ts)
        assert int(bucket.timestamp()) == BUCKET_BOUNDARY

    def test_next_bucket_boundary(self, builder):
        ts = make_ts(NEXT_BUCKET)
        bucket = builder._bucket_floor(ts)
        assert int(bucket.timestamp()) == NEXT_BUCKET

    def test_bucket_is_utc_aware(self, builder):
        ts = make_ts(BUCKET_BOUNDARY)
        bucket = builder._bucket_floor(ts)
        assert bucket.tzinfo is not None
        assert bucket.tzinfo == timezone.utc


# ---------------------------------------------------------------------------
# 2. First tick initialises OHLCV correctly
# ---------------------------------------------------------------------------

class TestFirstTick:
    def test_sets_ohlcv(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("BTC", 50000.0, 1.5, ts, "crypto", "binance")
        c = builder.candles[("BTC", "5m")]
        assert c.open == 50000.0
        assert c.high == 50000.0
        assert c.low == 50000.0
        assert c.close == 50000.0
        assert c.volume == 1.5
        assert c.trade_count == 1

    def test_sets_metadata(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("BTC", 50000.0, 1.5, ts, "crypto", "binance")
        c = builder.candles[("BTC", "5m")]
        assert c.symbol == "BTC"
        assert c.asset_class == "crypto"
        assert c.source == "binance"
        assert c.interval == "5m"

    def test_ts_bucket_is_floor(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("BTC", 50000.0, 1.5, ts, "crypto", "binance")
        c = builder.candles[("BTC", "5m")]
        assert int(c.ts_bucket.timestamp()) == BUCKET_BOUNDARY

    def test_active_symbols_increments(self, builder):
        ts = make_ts(MID_BUCKET)
        assert builder.active_symbols == 0
        builder.ingest("BTC", 50000.0, 1.0, ts, "crypto", "binance")
        assert builder.active_symbols == 1


# ---------------------------------------------------------------------------
# 3. Second tick updates OHLCV correctly
# ---------------------------------------------------------------------------

class TestSecondTick:
    def test_open_preserved(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("ETH", 2000.0, 1.0, ts, "crypto", "binance")
        builder.ingest("ETH", 2100.0, 1.0, make_ts(MID_BUCKET + 10), "crypto", "binance")
        c = builder.candles[("ETH", "5m")]
        assert c.open == 2000.0

    def test_close_updated(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("ETH", 2000.0, 1.0, ts, "crypto", "binance")
        builder.ingest("ETH", 2100.0, 1.0, make_ts(MID_BUCKET + 10), "crypto", "binance")
        c = builder.candles[("ETH", "5m")]
        assert c.close == 2100.0

    def test_high_updated(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("ETH", 2000.0, 1.0, ts, "crypto", "binance")
        builder.ingest("ETH", 2100.0, 1.0, make_ts(MID_BUCKET + 10), "crypto", "binance")
        c = builder.candles[("ETH", "5m")]
        assert c.high == 2100.0

    def test_volume_accumulated(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("ETH", 2000.0, 1.5, ts, "crypto", "binance")
        builder.ingest("ETH", 2100.0, 2.5, make_ts(MID_BUCKET + 10), "crypto", "binance")
        c = builder.candles[("ETH", "5m")]
        assert c.volume == pytest.approx(4.0)

    def test_trade_count_incremented(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("ETH", 2000.0, 1.0, ts, "crypto", "binance")
        builder.ingest("ETH", 2100.0, 1.0, make_ts(MID_BUCKET + 10), "crypto", "binance")
        c = builder.candles[("ETH", "5m")]
        assert c.trade_count == 2


# ---------------------------------------------------------------------------
# 4. Low updates
# ---------------------------------------------------------------------------

class TestLowUpdates:
    def test_low_drops_below_initial(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("SOL", 100.0, 1.0, ts, "crypto", "binance")
        builder.ingest("SOL", 80.0, 1.0, make_ts(MID_BUCKET + 5), "crypto", "binance")
        c = builder.candles[("SOL", "5m")]
        assert c.low == 80.0

    def test_low_does_not_update_when_higher(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("SOL", 100.0, 1.0, ts, "crypto", "binance")
        builder.ingest("SOL", 120.0, 1.0, make_ts(MID_BUCKET + 5), "crypto", "binance")
        c = builder.candles[("SOL", "5m")]
        assert c.low == 100.0

    def test_high_low_sequence(self, builder):
        make_ts(MID_BUCKET)
        for i, price in enumerate([100.0, 150.0, 75.0, 130.0]):
            builder.ingest("SOL", price, 1.0, make_ts(MID_BUCKET + i * 10), "crypto", "binance")
        c = builder.candles[("SOL", "5m")]
        assert c.high == 150.0
        assert c.low == 75.0
        assert c.open == 100.0
        assert c.close == 130.0


# ---------------------------------------------------------------------------
# 5. VWAP calculation
# ---------------------------------------------------------------------------

class TestVWAP:
    def test_vwap_formula(self, builder):
        # (100*2 + 200*3) / (2+3) = (200 + 600) / 5 = 160.0
        ts = make_ts(MID_BUCKET)
        builder.ingest("BTC", 100.0, 2.0, ts, "crypto", "binance")
        builder.ingest("BTC", 200.0, 3.0, make_ts(MID_BUCKET + 30), "crypto", "binance")
        c = builder.candles[("BTC", "5m")]
        assert c.vwap == pytest.approx(160.0)

    def test_vwap_single_tick(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("BTC", 50000.0, 2.0, ts, "crypto", "binance")
        c = builder.candles[("BTC", "5m")]
        assert c.vwap == pytest.approx(50000.0)

    def test_vwap_none_when_zero_volume(self):
        # Edge case: manually construct a CandleState with zero volume denominator
        cs = CandleState(
            symbol="X", asset_class="crypto", source="test",
            interval="5m", ts_bucket=make_ts(BUCKET_BOUNDARY),
            open=100.0, high=100.0, low=100.0, close=100.0,
            volume=0.0, vwap_numerator=0.0, vwap_denominator=0.0,
            trade_count=0,
        )
        assert cs.vwap is None


# ---------------------------------------------------------------------------
# 6. Bucket rollover
# ---------------------------------------------------------------------------

class TestBucketRollover:
    def test_rollover_pushes_to_flush_queue(self, builder):
        builder.ingest("BTC", 50000.0, 1.0, make_ts(MID_BUCKET), "crypto", "binance")
        assert builder.pending_flush == 0
        # Tick in the next bucket triggers rollover
        builder.ingest("BTC", 51000.0, 1.0, make_ts(NEXT_BUCKET + 10), "crypto", "binance")
        assert builder.pending_flush == 1

    def test_rollover_old_candle_is_correct(self, builder):
        builder.ingest("BTC", 50000.0, 2.0, make_ts(MID_BUCKET), "crypto", "binance")
        builder.ingest("BTC", 51000.0, 1.0, make_ts(NEXT_BUCKET + 10), "crypto", "binance")
        flushed = builder.flush_queue[0]
        assert flushed.open == 50000.0
        assert flushed.close == 50000.0
        assert flushed.volume == pytest.approx(2.0)
        assert int(flushed.ts_bucket.timestamp()) == BUCKET_BOUNDARY

    def test_new_candle_starts_fresh_after_rollover(self, builder):
        builder.ingest("BTC", 50000.0, 1.0, make_ts(MID_BUCKET), "crypto", "binance")
        builder.ingest("BTC", 51000.0, 0.5, make_ts(NEXT_BUCKET + 10), "crypto", "binance")
        c = builder.candles[("BTC", "5m")]
        assert c.open == 51000.0
        assert c.trade_count == 1
        assert int(c.ts_bucket.timestamp()) == NEXT_BUCKET

    def test_no_rollover_within_same_bucket(self, builder):
        builder.ingest("BTC", 50000.0, 1.0, make_ts(MID_BUCKET), "crypto", "binance")
        builder.ingest("BTC", 51000.0, 1.0, make_ts(END_BUCKET), "crypto", "binance")
        assert builder.pending_flush == 0
        assert builder.active_symbols == 1


# ---------------------------------------------------------------------------
# 7. drain()
# ---------------------------------------------------------------------------

class TestDrain:
    def test_drain_returns_flushed_candles(self, builder):
        builder.ingest("BTC", 50000.0, 1.0, make_ts(MID_BUCKET), "crypto", "binance")
        builder.ingest("BTC", 51000.0, 1.0, make_ts(NEXT_BUCKET + 10), "crypto", "binance")
        result = builder.drain()
        assert len(result) == 1
        assert result[0].symbol == "BTC"

    def test_drain_clears_flush_queue(self, builder):
        builder.ingest("BTC", 50000.0, 1.0, make_ts(MID_BUCKET), "crypto", "binance")
        builder.ingest("BTC", 51000.0, 1.0, make_ts(NEXT_BUCKET + 10), "crypto", "binance")
        builder.drain()
        assert builder.pending_flush == 0

    def test_drain_empty_returns_empty_list(self, builder):
        result = builder.drain()
        assert result == []

    def test_drain_does_not_affect_active_candles(self, builder):
        builder.ingest("BTC", 50000.0, 1.0, make_ts(MID_BUCKET), "crypto", "binance")
        builder.ingest("BTC", 51000.0, 1.0, make_ts(NEXT_BUCKET + 10), "crypto", "binance")
        builder.drain()
        assert builder.active_symbols == 1


# ---------------------------------------------------------------------------
# 8. flush_all()
# ---------------------------------------------------------------------------

class TestFlushAll:
    def test_flush_all_moves_active_to_queue(self, builder):
        builder.ingest("BTC", 50000.0, 1.0, make_ts(MID_BUCKET), "crypto", "binance")
        builder.ingest("ETH", 2000.0, 5.0, make_ts(MID_BUCKET), "crypto", "binance")
        assert builder.active_symbols == 2
        builder.flush_all()
        assert builder.pending_flush == 2

    def test_flush_all_clears_active_candles(self, builder):
        builder.ingest("BTC", 50000.0, 1.0, make_ts(MID_BUCKET), "crypto", "binance")
        builder.flush_all()
        assert builder.active_symbols == 0

    def test_flush_all_candles_data_preserved(self, builder):
        builder.ingest("BTC", 50000.0, 3.0, make_ts(MID_BUCKET), "crypto", "binance")
        builder.flush_all()
        flushed = builder.flush_queue[0]
        assert flushed.symbol == "BTC"
        assert flushed.open == 50000.0
        assert flushed.volume == pytest.approx(3.0)

    def test_flush_all_appends_to_existing_queue(self, builder):
        # First trigger a natural rollover to populate the queue
        builder.ingest("ETH", 2000.0, 1.0, make_ts(MID_BUCKET), "crypto", "binance")
        builder.ingest("ETH", 2100.0, 1.0, make_ts(NEXT_BUCKET + 10), "crypto", "binance")
        assert builder.pending_flush == 1
        # Now flush_all should add the active ETH candle on top
        builder.flush_all()
        assert builder.pending_flush == 2

    def test_flush_all_on_empty_builder_is_noop(self, builder):
        builder.flush_all()
        assert builder.pending_flush == 0
        assert builder.active_symbols == 0


# ---------------------------------------------------------------------------
# 9. Multiple symbols tracked independently
# ---------------------------------------------------------------------------

class TestMultipleSymbols:
    def test_separate_candles_per_symbol(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("BTC", 50000.0, 1.0, ts, "crypto", "binance")
        builder.ingest("ETH", 2000.0, 5.0, ts, "crypto", "binance")
        assert builder.active_symbols == 2
        assert ("BTC", "5m") in builder.candles
        assert ("ETH", "5m") in builder.candles

    def test_correct_asset_class_per_symbol(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("AAPL", 175.0, 100.0, ts, "equity", "yfinance")
        builder.ingest("BTC", 50000.0, 1.0, ts, "crypto", "binance")
        assert builder.candles[("AAPL", "5m")].asset_class == "equity"
        assert builder.candles[("BTC", "5m")].asset_class == "crypto"

    def test_symbols_roll_over_independently(self, builder):
        builder.ingest("BTC", 50000.0, 1.0, make_ts(MID_BUCKET), "crypto", "binance")
        builder.ingest("ETH", 2000.0, 5.0, make_ts(MID_BUCKET), "crypto", "binance")
        # Only BTC rolls over
        builder.ingest("BTC", 51000.0, 1.0, make_ts(NEXT_BUCKET + 10), "crypto", "binance")
        assert builder.pending_flush == 1
        assert builder.flush_queue[0].symbol == "BTC"
        # ETH candle still active
        assert ("ETH", "5m") in builder.candles

    def test_independent_ohlcv_values(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("BTC", 50000.0, 2.0, ts, "crypto", "binance")
        builder.ingest("ETH", 2000.0, 10.0, ts, "crypto", "binance")
        btc = builder.candles[("BTC", "5m")]
        eth = builder.candles[("ETH", "5m")]
        assert btc.open == 50000.0
        assert eth.open == 2000.0
        assert btc.volume == pytest.approx(2.0)
        assert eth.volume == pytest.approx(10.0)

    def test_source_stored_per_symbol(self, builder):
        ts = make_ts(MID_BUCKET)
        builder.ingest("AAPL", 175.0, 100.0, ts, "equity", "yfinance")
        builder.ingest("SOL", 150.0, 50.0, ts, "crypto", "dex")
        assert builder.candles[("AAPL", "5m")].source == "yfinance"
        assert builder.candles[("SOL", "5m")].source == "dex"
