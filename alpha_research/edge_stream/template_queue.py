"""The order the stream registers its template proposals in (one a weekday at most).

Kept out of :data:`families.EVALUATION_MODULES` on purpose: a queue entry added, moved
or removed changes no registration's code hash, so it abandons nothing. What a queue
entry registers is still checked whole by :func:`sanity.check` and sealed with its
family's hash.
"""

from __future__ import annotations

from .universe import SP500_20251231

#: What the stream registers first, in order (one new registration per weekday at most).
QUEUE: tuple[dict, ...] = (
    {"family": "IM1", "group": "metals", "instruments": ("GLD", "SLV"), "lane": "history"},
    {"family": "IM1", "group": "energy", "instruments": ("USO", "UNG"), "lane": "history"},
    {"family": "IM1", "group": "intl_equity", "instruments": ("EFA", "EEM", "EWJ", "FXI", "EWZ"), "lane": "history"},
    {"family": "IM1", "group": "crypto_etf", "instruments": ("IBIT", "ETHA"), "lane": "history"},
    {"family": "IM1", "group": "us_single_stocks",
     "instruments": ("AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA"), "lane": "history"},
    {"family": "IM1", "group": "fx", "instruments": ("FXE", "FXY", "UUP"), "lane": "history"},
    {"family": "IM1", "group": "us_equity_index", "instruments": ("SPY", "QQQ", "IWM"), "lane": "forward"},
    {"family": "EC1", "group": "sp500_events", "instruments": SP500_20251231, "lane": "history"},
)
