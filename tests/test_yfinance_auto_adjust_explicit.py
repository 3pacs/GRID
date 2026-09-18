"""Pin the ``auto_adjust`` basis at every live yfinance price call site.

yfinance flipped ``yf.download()``'s ``auto_adjust`` default from ``False`` to
``True`` during the 0.2.x line (deprecation warning in 0.2.53,
ranaroussi/yfinance#2230); ``Ticker.history()`` defaults to ``True`` as well.
With ``auto_adjust=True`` the ``Adj Close`` column disappears and the ``OHLC``
columns carry split/dividend **back-adjusted** prices instead of raw ones.

That flip is silent: every call site below reads ``df["Close"]``, which still
exists either way — it just quietly changes basis. The sites exercised here all
compare a yfinance price against a *raw* price from somewhere else:

  * ``raw_series`` ``"YF:{ticker}:close"`` — written by
    ``ingestion/yfinance_pull.py`` with an explicit ``auto_adjust=False`` (it
    stores the adjusted series separately as ``"YF:{ticker}:adj_close"``)
  * ``options_daily_signals.spot_price``
  * an option **strike** (``trading/options_tracker.py``)
  * ``fast_info.last_price`` / ``previous_close`` (raw quotes)

so a back-adjusted close on one side of the comparison manufactures a spurious
move equal to the cumulative adjustment factor between the two dates.

These tests assert the flag is passed explicitly, so a future yfinance default
change (either direction) cannot silently move the basis again.
"""

from __future__ import annotations

import os
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Importing the api routers pulls in config.Settings, whose startup validator
# rejects an empty DB_PASSWORD. Only relevant for local runs with no .env —
# setdefault leaves a real value (CI, .env) untouched, and this neither
# weakens nor bypasses that validator.
os.environ.setdefault("DB_PASSWORD", "test-password")


@pytest.fixture(autouse=True)
def _clear_yf_negative_cache():
    """Keep the module-level yfinance negative caches from leaking between tests."""
    from intelligence import trust_scorer

    trust_scorer._YF_NO_DATA.clear()
    yield
    trust_scorer._YF_NO_DATA.clear()


def _ohlc_frame(dates, close, *, adj_close=None, volume=1_000_000):
    """Minimal yfinance-shaped daily frame."""
    cols = {
        "Open": list(close),
        "High": [c * 1.01 for c in close],
        "Low": [c * 0.99 for c in close],
        "Close": list(close),
        "Volume": [volume] * len(close),
    }
    if adj_close is not None:
        cols["Adj Close"] = list(adj_close)
    return pd.DataFrame(cols, index=pd.to_datetime(dates))


# ── 1. intelligence/trust_scorer.py — signal outcome scoring ───────────────

def test_trust_scorer_price_fetch_passes_auto_adjust_false():
    """The last-resort price fetch must ask for raw closes explicitly."""
    from intelligence.trust_scorer import _fetch_yfinance_price

    frame = _ohlc_frame(["2026-03-10", "2026-03-11"], [180.0, 182.0],
                        adj_close=[178.0, 180.0])

    with patch("yfinance.download", return_value=frame) as mock_dl:
        price = _fetch_yfinance_price("AAPL", date(2026, 3, 11))

    assert price == pytest.approx(182.0)
    assert mock_dl.call_count == 1
    assert mock_dl.call_args.kwargs["auto_adjust"] is False


def test_trust_scorer_price_fetch_returns_raw_not_adjusted_close():
    """Regression: a dividend-adjusted close must never reach the scorer.

    ``_fetch_yfinance_price`` is the third leg of a price chain whose other two
    legs (``options_daily_signals.spot_price`` and ``raw_series
    "YF:{ticker}:close"``) are raw. Before the fix this leg silently returned
    back-adjusted closes, so an entry price sourced from the DB and an exit
    price sourced from here differed by the cumulative dividend/split factor —
    a fabricated return, against a MOVE_THRESHOLD_PCT of only 1.0%.
    """
    from intelligence.trust_scorer import _fetch_yfinance_price

    raw = [180.0, 182.0]
    adjusted = [176.4, 178.4]  # ~2% of dividends back-adjusted out

    def fake_download(*_args, **kwargs):
        # Mirror real yfinance: auto_adjust=True overwrites Close with the
        # adjusted series and drops Adj Close entirely.
        if kwargs.get("auto_adjust", True):
            return _ohlc_frame(["2026-03-10", "2026-03-11"], adjusted)
        return _ohlc_frame(["2026-03-10", "2026-03-11"], raw, adj_close=adjusted)

    with patch("yfinance.download", side_effect=fake_download):
        price = _fetch_yfinance_price("AAPL", date(2026, 3, 11))

    assert price == pytest.approx(182.0), (
        "trust_scorer must price off the raw close to stay on the same basis "
        "as raw_series YF:*:close and options_daily_signals.spot_price"
    )


# ── 2. trading/options_tracker.py — expired option scoring ─────────────────

def test_options_tracker_price_fetch_passes_auto_adjust_false():
    from trading.options_tracker import _fetch_yfinance_price

    frame = _ohlc_frame(["2026-03-10", "2026-03-11"], [180.0, 182.0],
                        adj_close=[178.0, 180.0])

    with patch("yfinance.download", return_value=frame) as mock_dl:
        price = _fetch_yfinance_price("AAPL", date(2026, 3, 11))

    assert price == pytest.approx(182.0)
    assert mock_dl.call_args.kwargs["auto_adjust"] is False


def test_options_tracker_raw_close_preserves_intrinsic_value_vs_strike():
    """Regression: intrinsic value is measured against a raw, unadjusted strike.

    ``score_expired_recommendations`` computes ``max(0, actual_price - strike)``
    for a CALL. Option strikes are raw prices, so a back-adjusted underlying
    close systematically understates CALL intrinsic (and overstates PUT
    intrinsic) — turning winners into ``EXPIRED`` around dividend events.
    """
    from trading.options_tracker import _fetch_yfinance_price

    raw = [180.0, 182.0]
    adjusted = [176.4, 178.4]
    strike = 180.0

    def fake_download(*_args, **kwargs):
        if kwargs.get("auto_adjust", True):
            return _ohlc_frame(["2026-03-10", "2026-03-11"], adjusted)
        return _ohlc_frame(["2026-03-10", "2026-03-11"], raw, adj_close=adjusted)

    with patch("yfinance.download", side_effect=fake_download):
        price = _fetch_yfinance_price("AAPL", date(2026, 3, 11))

    assert price is not None
    # Raw basis: the call finished $2 in the money. On the adjusted basis it
    # would have scored as expiring worthless.
    assert max(0.0, price - strike) == pytest.approx(2.0)


# ── 3. ingestion/altdata/crypto_etf_flows.py — signal entry prices ─────────

def test_crypto_etf_flows_passes_auto_adjust_false():
    """The close here is persisted as the signal's entry price."""
    from ingestion.altdata.crypto_etf_flows import CryptoETFPuller

    dates = pd.date_range("2026-02-01", periods=25, freq="D")
    # Flat volume keeps the spike ratio below 2.0, so no DB write is attempted.
    frame = _ohlc_frame(dates, [50.0 + i * 0.1 for i in range(25)])

    with patch("yfinance.download", return_value=frame) as mock_dl:
        result = CryptoETFPuller(engine=MagicMock()).pull()

    assert result["signals_emitted"] == 0
    assert mock_dl.call_count > 0
    for call in mock_dl.call_args_list:
        assert call.kwargs["auto_adjust"] is False


# ── 4. ingestion/realtime/feeds/yahoo.py — live tape ───────────────────────

def test_yahoo_realtime_feed_passes_auto_adjust_false():
    """The live tape must carry actual traded prices, not a back-adjusted series."""
    from ingestion.realtime.feeds.yahoo import SYMBOLS, _fetch_prices

    symbols = list(SYMBOLS.keys())
    idx = pd.to_datetime(["2026-03-11 14:30", "2026-03-11 14:31"])
    frame = pd.DataFrame(
        {
            ("Close", symbols[0]): [100.0, 101.0],
            ("Volume", symbols[0]): [5000, 5100],
        },
        index=idx,
    )
    frame.columns = pd.MultiIndex.from_tuples(frame.columns)

    with patch("yfinance.download", return_value=frame) as mock_dl:
        prices = _fetch_prices()

    assert mock_dl.call_count == 1
    assert mock_dl.call_args.kwargs["auto_adjust"] is False
    assert prices[symbols[0]][0] == pytest.approx(101.0)


# ── 5. api/routers/watchlist_helpers.py — displayed + DB-cached prices ─────

def test_watchlist_batch_fetch_passes_auto_adjust_false():
    """These closes are shown to the user and cached back into raw_series."""
    from api.routers.watchlist_helpers import _batch_fetch_prices

    frame = _ohlc_frame(
        ["2026-03-09", "2026-03-10", "2026-03-11"],
        [178.0, 180.0, 182.0],
        adj_close=[176.0, 178.0, 180.0],
    )

    with patch("yfinance.download", return_value=frame) as mock_dl:
        result = _batch_fetch_prices(["AAPL"])

    assert mock_dl.call_count == 1
    assert mock_dl.call_args.kwargs["auto_adjust"] is False
    assert result["AAPL"]["price"] == pytest.approx(182.0)


def test_watchlist_live_price_history_fallback_passes_auto_adjust_false():
    """The history() fallback must match the raw basis of the fast_info branch."""
    from api.routers.watchlist_helpers import _fetch_live_price

    frame = _ohlc_frame(["2026-03-10", "2026-03-11"], [180.0, 182.0])

    fake_info = MagicMock()
    fake_info.last_price = None      # force the history() fallback
    fake_info.previous_close = None

    fake_ticker = MagicMock()
    fake_ticker.fast_info = fake_info
    fake_ticker.history.return_value = frame

    with patch("yfinance.Ticker", return_value=fake_ticker):
        result = _fetch_live_price("AAPL")

    assert result is not None
    assert result["price"] == pytest.approx(182.0)
    assert fake_ticker.history.call_args.kwargs["auto_adjust"] is False


# ── 6. Guard: no live yfinance call site may rely on the library default ───

def test_no_unspecified_auto_adjust_in_live_price_paths():
    """Every yfinance price call in the live stack states its basis.

    Covers both ``yf.download(...)`` and ``Ticker(...).history(...)`` — the
    latter defaults to ``auto_adjust=True`` too, and
    ``api/routers/watchlist_analysis.py`` relied on that default while
    serving the result as ``price_source: "yfinance"`` *and* caching it into
    ``raw_series`` next to raw closes (audit C-M13).

    Scripted one-off loaders under ``scripts/`` are intentionally excluded —
    this guard covers the always-on service code.
    """
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    packages = ["api", "ingestion", "intelligence", "trading", "backtest"]

    offenders: list[str] = []
    call_re = re.compile(r"yf\.download\s*\(|\.history\s*\(", re.MULTILINE)

    for pkg in packages:
        for path in (root / pkg).rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            for match in call_re.finditer(text):
                # Skip prose mentions of yf.download() in comments/docstrings.
                line_start = text.rfind("\n", 0, match.start()) + 1
                prefix = text[line_start:match.start()]
                if "#" in prefix:
                    continue
                # Slice from the call to its closing paren (calls here are
                # never nested deeply enough to need a real parser).
                depth, i = 0, match.end() - 1
                while i < len(text):
                    if text[i] == "(":
                        depth += 1
                    elif text[i] == ")":
                        depth -= 1
                        if depth == 0:
                            break
                    i += 1
                call_src = text[match.start():i + 1]
                if "auto_adjust" not in call_src:
                    line = text[:match.start()].count("\n") + 1
                    offenders.append(f"{path.relative_to(root)}:{line}")

    assert offenders == [], (
        "yfinance call sites relying on the library default for "
        "auto_adjust (it is True in yfinance 0.2.x+, which silently returns "
        f"split/dividend-adjusted OHLC): {offenders}"
    )
