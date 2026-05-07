"""Tests for the realtime candle DB flusher."""

from datetime import datetime, timezone


from ingestion.realtime.candle_builder import CandleState
from ingestion.realtime.flusher import build_insert_values, MAX_BUFFER_CYCLES


class TestBuildInsertValues:
    def test_single_candle(self):
        candle = CandleState(
            symbol="BTCUSDT", asset_class="crypto", source="binance",
            interval="5m",
            ts_bucket=datetime(2026, 4, 6, 13, 30, 0, tzinfo=timezone.utc),
            open=70000.0, high=70500.0, low=69800.0, close=70200.0,
            volume=15.3, vwap_numerator=70200.0 * 15.3,
            vwap_denominator=15.3, trade_count=42,
        )
        rows = build_insert_values([candle])
        assert len(rows) == 1
        row = rows[0]
        assert row[0] == "BTCUSDT"
        assert row[1] == "crypto"
        assert row[2] == "5m"
        assert row[4] == 70000.0  # open
        assert row[5] == 70500.0  # high
        assert row[6] == 69800.0  # low
        assert row[7] == 70200.0  # close
        assert row[8] == 15.3     # volume
        assert row[10] == 42      # trade_count
        assert row[11] == "binance"  # source

    def test_empty_list(self):
        assert build_insert_values([]) == []

    def test_vwap_none_when_zero_volume(self):
        candle = CandleState(
            symbol="TEST", asset_class="crypto", source="binance",
            interval="5m",
            ts_bucket=datetime(2026, 4, 6, 13, 30, 0, tzinfo=timezone.utc),
            open=100.0, high=100.0, low=100.0, close=100.0,
            volume=0.0, vwap_numerator=0.0, vwap_denominator=0.0, trade_count=0,
        )
        rows = build_insert_values([candle])
        assert rows[0][9] is None  # vwap

    def test_multiple_candles(self):
        candles = [
            CandleState(
                symbol=f"SYM{i}", asset_class="crypto", source="binance",
                interval="5m",
                ts_bucket=datetime(2026, 4, 6, 13, 30, 0, tzinfo=timezone.utc),
                open=100.0, high=110.0, low=90.0, close=105.0,
                volume=10.0, vwap_numerator=1050.0, vwap_denominator=10.0,
                trade_count=5,
            )
            for i in range(5)
        ]
        rows = build_insert_values(candles)
        assert len(rows) == 5


class TestBufferConfig:
    def test_max_buffer_cycles(self):
        assert MAX_BUFFER_CYCLES == 12
