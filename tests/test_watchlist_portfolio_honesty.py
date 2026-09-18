"""Honesty regression tests for the watchlist portfolio + edge surfaces.

Covers the Batch-1 findings of the 2026-09-17 fake-data audit:

* **C-H1** — ``GET /api/v1/watchlist/portfolio`` multiplied a hardcoded
  ``ESTIMATED_PORTFOLIO = 125_000`` by each item's weight and served the result
  as ``total_value`` and as dollar ``total_pnl_1d`` / ``total_pnl_1m``. No
  position sizes are stored anywhere, so every one of those dollars was
  invented.
* **C-H2** — ``pct_1m`` was ``pct_1w * 4.0``, a 1-week return wearing a
  1-month label. ``_batch_fetch_prices`` only downloads a 5-day window, so no
  real 1-month close is ever loaded on this path.
* **C-M16** — a holding with no price contributed ``0`` P&L, making "we could
  not price this" indistinguishable from "it did not move".
* **C-M15** — ``beta_weighted`` was a per-asset-class lookup, not a beta.
* **C-H10 / C-H11 / C-M21** — ``GET /api/v1/watchlist/{t}/edge`` filled missing
  metadata with ``1.0`` dark-pool volume, a ``0.5`` market-implied probability,
  ``0`` insider shares/value and a ``$0`` whale strike/premium.
* **C-M12** — the overview's rule-based "Price Action" line printed a
  *fraction* with a ``%`` sign, rendering a +2.34% day as "up 0.0%".
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")

from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", _pwd_ctx.hash("testpassword123"))

from api.auth import create_token
from api.dependencies import get_db_engine
from api.main import app
from api.routers.watchlist_overview import _format_price_action

client = TestClient(app)

# Every dollar-denominated key the old endpoint emitted. None of these may come
# back while GRID stores no position sizes.
_FORBIDDEN_DOLLAR_KEYS = (
    "total_pnl_1d",
    "total_pnl_1d_pct",
    "total_pnl_1m",
    "total_pnl_1w",
)


def _auth_header() -> dict[str, str]:
    return {"Authorization": f"Bearer {create_token(expires_hours=1)}"}


def _portfolio_engine(watchlist_rows, options_row=None):
    """Mock engine for get_portfolio.

    ``watchlist_rows`` answers the ``SELECT ticker, display_name, ...`` fetchall;
    ``options_row`` answers the options_recommendations aggregate fetchone.
    """
    def _execute(statement, *args, **kwargs):
        sql = str(statement)
        result = MagicMock()
        if "FROM watchlist" in sql:
            result.fetchall.return_value = watchlist_rows
        elif "options_recommendations" in sql:
            result.fetchone.return_value = options_row
        else:
            result.fetchall.return_value = []
            result.fetchone.return_value = None
        return result

    conn = MagicMock()
    conn.execute.side_effect = _execute

    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def _get_portfolio(watchlist_rows, prices, options_row=None):
    """Drive GET /portfolio with a mocked DB and a mocked price cache."""
    with patch("api.routers.watchlist_core._init_table"), \
            patch("api.routers.watchlist_core.get_db_engine",
                  return_value=_portfolio_engine(watchlist_rows, options_row)), \
            patch("api.routers.watchlist_core._get_cached_prices", return_value=prices), \
            patch("api.routers.watchlist_core._batch_fetch_prices", return_value=prices):
        response = client.get("/api/v1/watchlist/portfolio", headers=_auth_header())
    assert response.status_code == 200, response.text
    return response.json()


# Three holdings, equal-weighted (weight column NULL).
_THREE_ITEMS = [
    ("AAPL", "Apple", "stock", None),
    ("BTC", "Bitcoin", "crypto", None),
    ("SPY", "S&P 500 ETF", "etf", None),
]

_ALL_PRICED = {
    "AAPL": {"price": 200.0, "pct_1d": 0.01, "pct_1w": 0.02},
    "BTC": {"price": 60000.0, "pct_1d": -0.02, "pct_1w": 0.05},
    "SPY": {"price": 500.0, "pct_1d": 0.004, "pct_1w": 0.01},
}


class TestNoSyntheticDollars:
    def test_no_synthetic_total_value(self):
        """The acceptance test: no stored portfolio value => total_value null,
        no dollar P&L keys, and a machine-readable reason."""
        data = _get_portfolio(_THREE_ITEMS, _ALL_PRICED)

        assert data["total_value"] is None
        assert data["total_value_basis"] == "no_position_sizes_stored"
        for key in _FORBIDDEN_DOLLAR_KEYS:
            assert key not in data, f"{key} must not be served"

        # And nothing anywhere in the payload is the old constant.
        assert "125000" not in response_text(data)
        assert "125_000" not in response_text(data)

    def test_no_dollar_pnl_on_individual_positions(self):
        """C-M16's per-position dollar P&L was weight x 125_000 x return."""
        data = _get_portfolio(_THREE_ITEMS, _ALL_PRICED)
        for pos in data["positions"]:
            assert "pnl_1d" not in pos
            assert "pnl_1m" not in pos
            assert "alloc_value" not in pos

    def test_empty_watchlist_is_null_not_zero(self):
        """Zero rows: an empty book still must not claim a $0 portfolio."""
        data = _get_portfolio([], {})

        assert data["total_value"] is None
        assert data["total_value_basis"] == "no_position_sizes_stored"
        assert data["positions"] == []
        assert data["positions_missing_price"] == 0
        assert data["weighted_return_1d_pct"] is None
        assert data["return_1d_weight_coverage"] is None
        for key in _FORBIDDEN_DOLLAR_KEYS:
            assert key not in data
        risk = data["risk_metrics"]
        assert risk["concentration_top3"] is None
        assert risk["beta_proxy_by_asset_class"] is None
        assert risk["sector_diversification_score"] is None


class TestNoExtrapolated1Month:
    def test_no_1m_return_anywhere(self):
        """C-H2: pct_1w * 4 is gone and no 1-month field is served in its place,
        because no 1-month close is loaded on this path."""
        data = _get_portfolio(_THREE_ITEMS, _ALL_PRICED)

        blob = response_text(data)
        assert "pct_1m" not in blob
        assert "change_1m" not in blob
        assert "total_pnl_1m" not in blob
        for pos in data["positions"]:
            assert "change_1m" not in pos

    def test_one_week_return_is_passed_through_unscaled(self):
        """The measured 1-week return survives untouched — it is real data."""
        data = _get_portfolio(_THREE_ITEMS, _ALL_PRICED)
        by_ticker = {p["ticker"]: p for p in data["positions"]}
        assert by_ticker["BTC"]["change_1w"] == 0.05


class TestMissingPriceCoverage:
    def test_missing_price_positions_excluded_and_counted(self):
        """Acceptance 4: partial coverage. len(positions) + positions_missing_price
        == len(items), and the unpriced name is in no total."""
        prices = {
            "AAPL": {"price": 200.0, "pct_1d": 0.01, "pct_1w": 0.02},
            "SPY": {"price": 500.0, "pct_1d": 0.004, "pct_1w": 0.01},
            # BTC absent: yfinance returned nothing for it.
        }
        data = _get_portfolio(_THREE_ITEMS, prices)

        assert len(data["positions"]) + data["positions_missing_price"] == len(_THREE_ITEMS)
        assert data["positions_missing_price"] == 1
        assert data["missing_price_tickers"] == ["BTC"]
        assert {p["ticker"] for p in data["positions"]} == {"AAPL", "SPY"}

        # The unpriced holding is in no total: not in the allocation, not in
        # the weight that was actually priced, not in the risk metrics.
        assert "Crypto" not in data["allocation"]["by_sector"]
        assert "crypto" not in data["allocation"]["by_asset_type"]
        assert data["weight_priced_total"] < 1.0

        # The weighted 1d return is over the two priced names only, and is NOT
        # dragged toward zero by the missing one.
        expected = (0.01 + 0.004) / 2
        assert abs(data["weighted_return_1d_pct"] - expected) < 1e-9

    def test_no_prices_at_all(self):
        """Stale/failed price fetch: every holding unpriced. Nothing is totalled."""
        data = _get_portfolio(_THREE_ITEMS, {})

        assert data["positions"] == []
        assert data["positions_missing_price"] == 3
        assert sorted(data["missing_price_tickers"]) == ["AAPL", "BTC", "SPY"]
        assert data["weighted_return_1d_pct"] is None
        assert data["return_1d_weight_coverage"] is None
        assert data["weight_priced_total"] == 0.0
        assert data["risk_metrics"]["concentration_top3"] is None
        assert data["risk_metrics"]["beta_proxy_by_asset_class"] is None
        assert data["risk_metrics"]["sector_diversification_score"] is None

    def test_explicit_null_price_counts_as_missing(self):
        """A price key present but null is still 'we could not price this'."""
        prices = dict(_ALL_PRICED)
        prices["BTC"] = {"price": None, "pct_1d": None, "pct_1w": None}
        data = _get_portfolio(_THREE_ITEMS, prices)

        assert data["positions_missing_price"] == 1
        assert data["missing_price_tickers"] == ["BTC"]

    def test_priced_but_no_return_is_not_counted_as_flat(self):
        """A holding with a price but no 1d return must not enter the weighted
        return as a 0% move; it only shrinks the coverage."""
        prices = {
            "AAPL": {"price": 200.0, "pct_1d": 0.02, "pct_1w": 0.02},
            "BTC": {"price": 60000.0, "pct_1d": None, "pct_1w": None},
            "SPY": {"price": 500.0, "pct_1d": 0.02, "pct_1w": 0.01},
        }
        data = _get_portfolio(_THREE_ITEMS, prices)

        assert len(data["positions"]) == 3
        assert data["positions_missing_price"] == 0
        # Both contributing names moved +2%, so the weighted return is +2% —
        # not the +1.33% a zero-filled third position would have produced.
        assert abs(data["weighted_return_1d_pct"] - 0.02) < 1e-9
        assert abs(data["return_1d_weight_coverage"] - 2 / 3) < 1e-3

    def test_custom_weights_respected_in_weighted_return(self):
        items = [
            ("AAPL", "Apple", "stock", 3.0),
            ("SPY", "S&P 500 ETF", "etf", 1.0),
        ]
        prices = {
            "AAPL": {"price": 200.0, "pct_1d": 0.04, "pct_1w": 0.0},
            "SPY": {"price": 500.0, "pct_1d": 0.00, "pct_1w": 0.0},
        }
        data = _get_portfolio(items, prices)
        # 0.75 * 4% + 0.25 * 0% = 3%
        assert abs(data["weighted_return_1d_pct"] - 0.03) < 1e-6
        assert abs(data["return_1d_weight_coverage"] - 1.0) < 1e-6


class TestBetaProxyNaming:
    def test_beta_weighted_field_is_gone(self):
        """C-M15: the old name must not remain as a field."""
        data = _get_portfolio(_THREE_ITEMS, _ALL_PRICED)
        assert "beta_weighted" not in data["risk_metrics"]
        assert "beta_weighted" not in response_text(data)

    def test_beta_proxy_carries_a_basis_note(self):
        data = _get_portfolio(_THREE_ITEMS, _ALL_PRICED)
        risk = data["risk_metrics"]
        assert risk["beta_proxy_by_asset_class"] is not None
        basis = risk["beta_proxy_basis"]
        assert "asset_class_lookup" in basis
        # The note must say plainly that this is not a regression vs a benchmark.
        assert "SPY" in basis

    def test_beta_proxy_uses_the_documented_table(self):
        """1/3 * (1.1 stock + 1.8 crypto + 1.0 etf) = 1.3."""
        data = _get_portfolio(_THREE_ITEMS, _ALL_PRICED)
        assert abs(data["risk_metrics"]["beta_proxy_by_asset_class"] - 1.3) < 0.01


def response_text(payload) -> str:
    import json
    return json.dumps(payload)


# ══════════════════════════════════════════════════════════════════
#  GET /api/v1/watchlist/{ticker}/edge
# ══════════════════════════════════════════════════════════════════


def _edge_engine(whale_rows=(), social_rows=(), pred_rows=()):
    def _execute(statement, *args, **kwargs):
        sql = str(statement)
        result = MagicMock()
        if "source_type = 'scanner'" in sql:
            result.fetchall.return_value = list(whale_rows)
        elif "source_type = 'social'" in sql:
            result.fetchall.return_value = list(social_rows)
        elif "'prediction', 'polymarket'" in sql:
            result.fetchall.return_value = list(pred_rows)
        else:
            result.fetchall.return_value = []
            result.fetchone.return_value = None
        return result

    conn = MagicMock()
    conn.execute.side_effect = _execute
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def _get_edge(edge_data, whale_rows=(), social_rows=(), pred_rows=()):
    engine = _edge_engine(whale_rows, social_rows, pred_rows)
    app.dependency_overrides[get_db_engine] = lambda: engine
    try:
        with patch("intelligence.trust_scorer.get_insider_edge", return_value=edge_data), \
                patch("intelligence.trust_scorer.detect_convergence", return_value=[]):
            response = client.get("/api/v1/watchlist/NVDA/edge", headers=_auth_header())
    finally:
        app.dependency_overrides.pop(get_db_engine, None)
    assert response.status_code == 200, response.text
    return response.json()


class TestEdgeMissingMetadata:
    def test_dark_pool_volume_vs_avg_is_null_when_unmeasured(self):
        """C-H10: 1.0 read as 'exactly average volume', a measurement never taken."""
        data = _get_edge({
            "congressional": [],
            "insider": [],
            # metadata carries the direction but no volume_vs_avg.
            "darkpool": [{"direction": "BUY", "date": "2026-09-15", "metadata": {}}],
        })
        assert data["dark_pool"]["volume_vs_avg"] is None
        assert data["dark_pool"]["signal"] == "accumulation"

    def test_dark_pool_volume_vs_avg_passes_real_values_through(self):
        data = _get_edge({
            "congressional": [],
            "insider": [],
            "darkpool": [{
                "direction": "BUY", "date": "2026-09-15",
                "metadata": {"volume_vs_avg": 2.4},
            }],
        })
        assert data["dark_pool"]["volume_vs_avg"] == 2.4

    def test_prediction_market_probability_is_null_when_unquoted(self):
        """C-H11: 0.5 / 0.0 are tradeable-looking numbers no market quoted."""
        data = _get_edge(
            {"congressional": [], "insider": [], "darkpool": []},
            pred_rows=[("polymarket:nvda-500", "2026-09-15", {})],
        )
        assert len(data["prediction_markets"]) == 1
        market = data["prediction_markets"][0]
        assert market["probability"] is None
        assert market["change_24h"] is None

    def test_prediction_market_real_quote_survives(self):
        data = _get_edge(
            {"congressional": [], "insider": [], "darkpool": []},
            pred_rows=[(
                "polymarket:nvda-500", "2026-09-15",
                {"market": "NVDA > 500", "probability": 0.62, "change_24h": -0.03},
            )],
        )
        market = data["prediction_markets"][0]
        assert market["probability"] == 0.62
        assert market["change_24h"] == -0.03

    def test_insider_shares_and_value_are_null_when_absent(self):
        """C-M21: a '0 shares / $0' insider row reads as an observed zero-size trade."""
        data = _get_edge({
            "congressional": [],
            "insider": [{
                "insider": "Jane Doe", "direction": "SELL",
                "date": "2026-09-10", "metadata": {"title": "CFO"},
            }],
            "darkpool": [],
        })
        row = data["insider"][0]
        assert row["shares"] is None
        assert row["value"] is None
        assert row["name"] == "Jane Doe"

    def test_whale_strike_and_premium_are_null_when_absent(self):
        data = _get_edge(
            {"congressional": [], "insider": [], "darkpool": []},
            whale_rows=[("scanner:1", "CALL", "2026-09-15", {})],
        )
        row = data["whale_flow"][0]
        assert row["strike"] is None
        assert row["premium"] is None
        assert row["expiry"] is None

    def test_edge_with_no_signals_at_all(self):
        """Zero rows everywhere: empty lists, never a fabricated placeholder row."""
        data = _get_edge({"congressional": [], "insider": [], "darkpool": []})
        assert data["insider"] == []
        assert data["whale_flow"] == []
        assert data["prediction_markets"] == []
        assert data["dark_pool"] is None


# ══════════════════════════════════════════════════════════════════
#  C-M12 — fraction printed as a percent
# ══════════════════════════════════════════════════════════════════


class TestOverviewPercentFormatting:
    def test_fraction_is_rendered_as_a_percent(self):
        """pct_1d is a fraction; +2.34% must not print as 'up 0.0%'."""
        body = _format_price_action("NVDA", 500.0, 0.0234)
        assert "up 2.3% on the day" in body
        assert "0.0%" not in body

    def test_negative_day(self):
        body = _format_price_action("NVDA", 500.0, -0.0181)
        assert "down 1.8% on the day" in body

    def test_small_but_nonzero_move_still_rounds_honestly(self):
        body = _format_price_action("NVDA", 500.0, 0.0004)
        assert "up 0.0% on the day" in body

    def test_no_return_means_no_sentence(self):
        body = _format_price_action("NVDA", 500.0, None)
        assert "on the day" not in body
        assert "$500.00" in body
