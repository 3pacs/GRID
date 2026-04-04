"""
GRID — Sanity range definitions for data ingestion validation.

Each series family has a plausible (min, max) range.  Individual series
can override via SERIES_OVERRIDES.  Used by BasePuller.validate_row().
"""

from __future__ import annotations


# ── Per-family default ranges ─────────────────────────────────────────────

FAMILY_RANGES: dict[str, tuple[float, float]] = {
    "equity_price":      (0.01, 100_000.0),
    "equity_index":      (100.0, 100_000.0),
    "volatility":        (0.0, 200.0),
    "yield":             (-5.0, 25.0),
    "macro_rate":        (-10.0, 30.0),
    "crypto_price":      (0.0, 1_000_000.0),
    "crypto_metric":     (0.0, 100_000_000_000_000.0),
    "commodity_price":   (0.01, 100_000.0),
    "sentiment_score":   (-100.0, 100.0),
    "sentiment_pct":     (0.0, 100.0),
    "ratio":             (-100.0, 100.0),
    "spread":            (-20.0, 50.0),
    "fx_rate":           (0.001, 10_000.0),
    "fx_index":          (50.0, 200.0),
    "flow":              (-1e15, 1e15),
    "count":             (0.0, 1e12),
    "percentage":        (-200.0, 200.0),
    "index":             (0.0, 100_000.0),
    "binary":            (0.0, 1.0),
    "angle":             (0.0, 360.0),
}

# ── Series-level overrides (series_id -> (min, max)) ─────────────────────

SERIES_OVERRIDES: dict[str, tuple[float, float]] = {
    # VIX and vol
    "VIXCLS":             (5.0, 100.0),
    "vix_spot":           (5.0, 100.0),
    "vix_spot_yf":        (5.0, 100.0),
    "vvix":               (50.0, 250.0),

    # SPX / SPY
    "sp500_close":        (1_000.0, 15_000.0),
    "YF:^GSPC:close":     (1_000.0, 15_000.0),
    "spy_full":           (50.0, 1_500.0),

    # Yields
    "DFF":                (0.0, 25.0),
    "DGS1":               (0.0, 20.0),
    "DGS2":               (0.0, 20.0),
    "DGS5":               (0.0, 20.0),
    "DGS30":              (0.0, 20.0),
    "DFII10":             (-5.0, 15.0),
    "T10Y2Y":             (-5.0, 5.0),
    "T10Y3M":             (-5.0, 5.0),
    "T5YIE":              (-2.0, 10.0),
    "T10YIE":             (-2.0, 10.0),

    # FX
    "dxy_index":          (70.0, 140.0),
    "YF:DX-Y.NYB:close":  (70.0, 140.0),

    # Macro
    "CPIAUCSL":           (100.0, 600.0),
    "UNRATE":             (2.0, 30.0),
    "PAYEMS":             (50_000.0, 250_000_000.0),
    "UMCSENT":            (20.0, 120.0),

    # Credit
    "BAMLH0A0HYM2":      (1.0, 30.0),
    "BAMLC0A0CM":         (0.1, 10.0),

    # Crypto
    "btc_full":           (100.0, 1_000_000.0),
    "btc_usd_full":       (100.0, 1_000_000.0),
    "eth_full":           (1.0, 100_000.0),
    "eth_usd_full":       (1.0, 100_000.0),
    "sol_full":           (0.01, 10_000.0),
    "btc_dominance":      (10.0, 90.0),
    "crypto_fear_greed":  (0.0, 100.0),

    # Commodities
    "gold_futures_close": (200.0, 15_000.0),
    "YF:GC=F:close":      (200.0, 15_000.0),
    "YF:HG=F:close":      (0.5, 20.0),
    "YF:CL=F:close":      (0.0, 300.0),

    # Sentiment
    "spy_rsi":            (0.0, 100.0),
    "spy_pe_ratio":       (3.0, 80.0),
    "feargreed_cnn_value": (0.0, 100.0),
}

# ── Series -> family mapping hints ────────────────────────────────────────

SERIES_FAMILY_HINTS: dict[str, str] = {
    "VIXCLS": "volatility",
    "vix_spot": "volatility",
    "vvix": "volatility",
    "DFF": "macro_rate",
    "DGS1": "yield",
    "DGS2": "yield",
    "DGS5": "yield",
    "DGS30": "yield",
    "DFII10": "yield",
    "T10Y2Y": "spread",
    "T10Y3M": "spread",
    "T5YIE": "yield",
    "T10YIE": "yield",
    "CPIAUCSL": "index",
    "UNRATE": "percentage",
    "PAYEMS": "count",
    "UMCSENT": "sentiment_score",
    "BAMLH0A0HYM2": "spread",
    "BAMLC0A0CM": "spread",
    "spy_rsi": "sentiment_pct",
    "crypto_fear_greed": "sentiment_pct",
}


# ── Maximum percentage change before flagging ─────────────────────────────

MAX_PCT_CHANGE = 50.0  # flag if value changed >50% from prior observation


def get_range_for_series(
    series_id: str,
    family: str | None = None,
) -> tuple[float, float] | None:
    """Return (min, max) plausible range for a series.

    Checks SERIES_OVERRIDES first, then falls back to FAMILY_RANGES
    via SERIES_FAMILY_HINTS or the explicit family parameter.

    Returns None if no range is known.
    """
    if series_id in SERIES_OVERRIDES:
        return SERIES_OVERRIDES[series_id]

    effective_family = SERIES_FAMILY_HINTS.get(series_id, family)
    if effective_family and effective_family in FAMILY_RANGES:
        return FAMILY_RANGES[effective_family]

    return None
