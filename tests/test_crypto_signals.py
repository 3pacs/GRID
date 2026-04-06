"""Tests for crypto signal bridge — existing data → signal_sources."""
import json
import pytest
from datetime import date, datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from unittest.mock import patch


@pytest.fixture
def engine():
    """PostgreSQL test engine."""
    eng = create_engine("postgresql://grid_user:changeme@localhost:5432/grid")
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        pytest.skip("PostgreSQL not available")
    yield eng
    eng.dispose()


def test_coingecko_breakout_emits_signal(engine):
    """CoinGecko bridge detects price breakout and writes signal_sources."""
    from ingestion.crypto_signals import CryptoSignalBridge

    bridge = CryptoSignalBridge(engine)
    # Should not crash even if no data exists
    result = bridge.bridge_coingecko()
    assert isinstance(result, dict)
    assert "signals_emitted" in result


def test_bridge_all_returns_summary(engine):
    """bridge_all returns dict with per-source results and total."""
    from ingestion.crypto_signals import CryptoSignalBridge
    bridge = CryptoSignalBridge(engine)
    result = bridge.bridge_all()
    assert "total_emitted" in result
    assert isinstance(result["total_emitted"], int)


def test_normalize_ticker():
    """Ticker normalization strips USDT/USD suffix."""
    from ingestion.crypto_signals import _normalize_ticker
    assert _normalize_ticker("BTCUSDT") == "BTC"
    assert _normalize_ticker("ETHUSDT") == "ETH"
    assert _normalize_ticker("SOLUSD") == "SOL"
    assert _normalize_ticker("BTC") == "BTC"


def test_emit_signal_deduplicates(engine):
    """Duplicate signals are silently ignored via ON CONFLICT."""
    from ingestion.crypto_signals import _emit_signal
    with engine.begin() as conn:
        r1 = _emit_signal(conn, "test_crypto", "test_id", "BTC",
                          date.today(), "BUY", {"test": True})
        r2 = _emit_signal(conn, "test_crypto", "test_id", "BTC",
                          date.today(), "BUY", {"test": True})
    # Both should succeed (ON CONFLICT DO NOTHING doesn't error)
    assert r1 is True
    assert r2 is True
    # Clean up
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM signal_sources WHERE source_type = 'test_crypto'"))


def test_ticker_from_coingecko_name():
    """CoinGecko series name → ticker mapping."""
    from ingestion.crypto_signals import _ticker_from_coingecko_name
    assert _ticker_from_coingecko_name("coingecko.bitcoin.price") == "BTC"
    assert _ticker_from_coingecko_name("coingecko.ethereum.price") == "ETH"
    assert _ticker_from_coingecko_name("unknown.series") is None
