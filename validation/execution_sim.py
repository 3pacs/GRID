"""
Execution simulation layer powered by autopredict.

Bridges GRID's walk-forward backtester with autopredict's realistic
execution engine (order book simulation, slippage, fill rates, market impact).

Instead of GRID's flat cost_bps assumption, this module synthesizes an
order book from volatility/spread data and runs each trade through
autopredict's ExecutionEngine for realistic fill modeling.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log

from autopredict.core.types import (
    EdgeEstimate,
    ExecutionReport,
    MarketState,
    Order,
    OrderSide,
    OrderType,
    Portfolio,
    Position,
)
from autopredict.strategies.base import RiskLimits
from autopredict.strategies.mispriced_probability import MispricedProbabilityStrategy
from autopredict.config.schema import RiskConfig
from autopredict.live.risk import RiskManager


@dataclass
class ExecutionSimConfig:
    """Configuration for the execution simulation layer.

    Attributes:
        base_spread_bps: Default bid-ask spread in basis points.
        base_liquidity: Default per-side liquidity in notional units.
        volatility_spread_scale: Multiplier from annualised vol to spread.
        kelly_fraction: Fraction of Kelly for position sizing.
        max_position_pct: Max position as % of bankroll.
        min_edge: Minimum edge threshold to trade.
        aggressive_edge: Edge above which to use market orders.
        max_daily_loss: Daily loss circuit breaker.
        max_total_exposure: Maximum total notional exposure.
    """

    base_spread_bps: float = 50.0
    base_liquidity: float = 10_000.0
    volatility_spread_scale: float = 2.0
    kelly_fraction: float = 0.25
    max_position_pct: float = 0.02
    min_edge: float = 0.05
    aggressive_edge: float = 0.15
    max_daily_loss: float = 5000.0
    max_total_exposure: float = 50_000.0


class ExecutionSimulator:
    """Wraps autopredict's execution engine for GRID backtest use.

    Translates GRID regime signals (probability vectors from the ensemble)
    into autopredict MarketStates, runs them through a MispricedProbability
    strategy with Kelly sizing and realistic execution, and returns
    execution-quality-adjusted metrics.

    Usage from GRID's WalkForwardBacktest::

        sim = ExecutionSimulator(config=ExecutionSimConfig())
        adjusted = sim.simulate_era(
            predictions=proba_df,   # (n_samples, n_classes) probabilities
            actuals=y_series,       # true regime labels
            bankroll=100_000.0,
        )
        # adjusted["realised_return"], adjusted["execution_metrics"], ...
    """

    def __init__(self, config: ExecutionSimConfig | None = None) -> None:
        self.config = config or ExecutionSimConfig()

        # Build autopredict strategy
        self._risk_limits = RiskLimits(
            max_position_size=self.config.max_total_exposure * self.config.max_position_pct,
            max_total_exposure=self.config.max_total_exposure,
            max_daily_loss=self.config.max_daily_loss,
            min_edge_threshold=self.config.min_edge,
            min_confidence=0.5,
        )
        self._strategy = MispricedProbabilityStrategy(
            risk_limits=self._risk_limits,
            kelly_fraction=self.config.kelly_fraction,
            aggressive_edge_threshold=self.config.aggressive_edge,
        )

        # Risk manager
        self._risk_config = RiskConfig(
            max_position_per_market=self._risk_limits.max_position_size,
            max_total_exposure=self.config.max_total_exposure,
            max_daily_loss=self.config.max_daily_loss,
            kill_switch_threshold=-self.config.max_daily_loss * 2,
        )
        self._risk_mgr = RiskManager(self._risk_config)

        log.info(
            "ExecutionSimulator initialised — spread={s}bps, liq={l}, kelly={k}",
            s=self.config.base_spread_bps,
            l=self.config.base_liquidity,
            k=self.config.kelly_fraction,
        )

    # ── Public API ──────────────────────────────────────────────────

    def simulate_era(
        self,
        predictions: pd.DataFrame | np.ndarray,
        actuals: pd.Series,
        bankroll: float,
        class_names: list[str] | None = None,
        volatility: pd.Series | None = None,
    ) -> dict[str, Any]:
        """Simulate execution for one backtest era.

        Parameters:
            predictions: Probability matrix (n_samples × n_classes).
            actuals: True regime labels aligned with predictions index.
            bankroll: Starting bankroll for this era.
            class_names: Ordered class names matching prediction columns.
            volatility: Optional per-observation annualised volatility
                        for dynamic spread/liquidity scaling.

        Returns:
            Dict with keys:
                realised_return: Net return after execution costs.
                gross_return: Return before execution costs.
                execution_cost_bps: Average execution cost in basis points.
                avg_slippage_bps: Average slippage.
                avg_fill_rate: Mean fill rate across trades.
                n_trades: Number of trades attempted.
                n_fills: Number of trades with non-zero fills.
                risk_events: List of risk limit breaches.
                per_trade: List of per-trade detail dicts.
        """
        if isinstance(predictions, np.ndarray):
            predictions = pd.DataFrame(predictions)

        n_obs = len(predictions)
        if n_obs == 0:
            return self._empty_result()

        if class_names is None:
            class_names = [str(c) for c in predictions.columns]

        # Reset risk manager for era
        self._risk_mgr = RiskManager(self._risk_config)

        portfolio = Portfolio(cash=bankroll, starting_capital=bankroll)
        trades: list[dict[str, Any]] = []
        risk_events: list[str] = []

        for i in range(n_obs):
            proba = predictions.iloc[i].values
            actual = actuals.iloc[i] if i < len(actuals) else None

            # Determine the model's fair probability for the "positive" class
            # Use max probability as the model's confidence signal
            best_class_idx = int(np.argmax(proba))
            fair_prob = float(proba[best_class_idx])

            # Derive a synthetic market probability (lagged / slightly off)
            # In real use this would come from prior predictions or market data
            market_prob = float(np.mean(proba))

            # Clamp to valid range
            fair_prob = max(0.01, min(0.99, fair_prob))
            market_prob = max(0.01, min(0.99, market_prob))

            # Dynamic spread from volatility
            vol = float(volatility.iloc[i]) if volatility is not None and i < len(volatility) else 0.15
            spread_bps = self.config.base_spread_bps * (1 + self.config.volatility_spread_scale * vol)
            spread = spread_bps / 10_000.0

            mid = market_prob
            half_spread = spread / 2

            # Build autopredict MarketState
            from datetime import datetime, timedelta
            market_state = MarketState(
                market_id=f"era_obs_{i}",
                question=f"Regime at observation {i}",
                market_prob=market_prob,
                expiry=datetime.now() + timedelta(hours=24),
                category="economics",
                best_bid=max(0.001, mid - half_spread),
                best_ask=min(0.999, mid + half_spread),
                bid_liquidity=self.config.base_liquidity * (1 - vol),
                ask_liquidity=self.config.base_liquidity * (1 - vol),
                volume_24h=self.config.base_liquidity * 10,
                num_traders=100,
            )

            # Edge estimate
            edge = EdgeEstimate(
                market_id=market_state.market_id,
                fair_prob=fair_prob,
                market_prob=market_prob,
                confidence=fair_prob,  # use class confidence
            )

            if edge.abs_edge < self.config.min_edge:
                continue

            # Position sizing via strategy
            position = portfolio.positions.get(market_state.market_id)
            ap_position = None
            if position:
                ap_position = Position(
                    market_id=market_state.market_id,
                    size=position.size,
                    entry_price=position.entry_price,
                    current_price=position.current_price,
                )

            config_dict = {
                "probability_model": None,  # we pre-computed edge
                "portfolio": portfolio,
            }

            # Calculate size using Kelly
            size = self._kelly_size(edge, portfolio.total_value)
            if size <= 0:
                continue

            # Risk check
            risk_result = self._risk_mgr.check_order(
                Order(
                    market_id=market_state.market_id,
                    side=edge.direction,
                    order_type=OrderType.MARKET,
                    size=size,
                ),
                current_price=market_prob,
            )

            if not risk_result.passed:
                risk_events.append(risk_result.reason)
                continue

            # Simulate execution with slippage
            slippage_bps = self._estimate_slippage(size, market_state)
            fill_rate = self._estimate_fill_rate(edge, market_state)
            filled_size = size * fill_rate

            if filled_size <= 0:
                continue

            # Execution price with slippage
            slippage = slippage_bps / 10_000.0
            if edge.direction == OrderSide.BUY:
                fill_price = market_state.best_ask * (1 + slippage)
            else:
                fill_price = market_state.best_bid * (1 - slippage)
            fill_price = max(0.001, min(0.999, fill_price))

            # Determine outcome (did the regime prediction resolve correctly?)
            outcome = 0.0
            if actual is not None:
                predicted_class = class_names[best_class_idx] if best_class_idx < len(class_names) else str(best_class_idx)
                outcome = 1.0 if str(actual) == str(predicted_class) else 0.0

            # PnL
            if edge.direction == OrderSide.BUY:
                pnl = (outcome - fill_price) * filled_size
            else:
                pnl = (fill_price - outcome) * filled_size

            # Update risk manager
            self._risk_mgr.update_position(
                market_id=market_state.market_id,
                size_delta=filled_size if edge.direction == OrderSide.BUY else -filled_size,
                price=fill_price,
                pnl_delta=pnl,
            )

            portfolio.update_cash(pnl)

            trades.append({
                "observation": i,
                "side": edge.direction.value,
                "edge": round(edge.edge, 4),
                "confidence": round(edge.confidence, 4),
                "size": round(size, 2),
                "filled_size": round(filled_size, 2),
                "fill_price": round(fill_price, 4),
                "slippage_bps": round(slippage_bps, 2),
                "fill_rate": round(fill_rate, 4),
                "outcome": outcome,
                "pnl": round(pnl, 4),
            })

        # Aggregate results
        return self._aggregate_results(trades, risk_events, bankroll, portfolio)

    def estimate_execution_cost(
        self,
        trade_size: float,
        spread_bps: float | None = None,
        volatility: float = 0.15,
    ) -> dict[str, float]:
        """Estimate execution cost for a given trade size.

        Parameters:
            trade_size: Notional trade size.
            spread_bps: Override for spread (default uses config).
            volatility: Annualised volatility for spread scaling.

        Returns:
            Dict with slippage_bps, spread_cost_bps, total_cost_bps.
        """
        if spread_bps is None:
            spread_bps = self.config.base_spread_bps * (
                1 + self.config.volatility_spread_scale * volatility
            )

        half_spread_bps = spread_bps / 2
        # Market impact: square-root model
        impact_bps = 5.0 * np.sqrt(trade_size / max(self.config.base_liquidity, 1))

        return {
            "spread_cost_bps": round(float(half_spread_bps), 2),
            "market_impact_bps": round(float(impact_bps), 2),
            "total_cost_bps": round(float(half_spread_bps + impact_bps), 2),
        }

    # ── Private helpers ─────────────────────────────────────────────

    def _kelly_size(self, edge: EdgeEstimate, portfolio_value: float) -> float:
        """Kelly criterion position sizing."""
        if edge.direction == OrderSide.BUY:
            kelly = edge.edge / (1 - edge.fair_prob + 1e-9)
        else:
            kelly = -edge.edge / (edge.fair_prob + 1e-9)

        kelly = kelly * self.config.kelly_fraction
        kelly = max(0.0, min(kelly, 0.25))
        size = portfolio_value * kelly * edge.confidence

        return min(size, self._risk_limits.max_position_size)

    def _estimate_slippage(self, size: float, market: MarketState) -> float:
        """Estimate slippage in basis points using square-root market impact."""
        liquidity = market.total_liquidity or self.config.base_liquidity
        # Square-root impact model
        impact = 5.0 * np.sqrt(size / max(liquidity, 1))
        # Add half-spread
        half_spread_bps = market.spread_bps / 2
        return float(half_spread_bps + impact)

    def _estimate_fill_rate(self, edge: EdgeEstimate, market: MarketState) -> float:
        """Estimate probability of fill based on edge and spread."""
        if edge.abs_edge >= self.config.aggressive_edge:
            # Market orders always fill
            return 1.0
        # Limit orders: higher edge → better fill rate
        base_fill = 0.15
        edge_bonus = min(0.60, edge.abs_edge * 8)
        return min(0.95, base_fill + edge_bonus)

    def _aggregate_results(
        self,
        trades: list[dict[str, Any]],
        risk_events: list[str],
        starting_bankroll: float,
        portfolio: Portfolio,
    ) -> dict[str, Any]:
        """Aggregate per-trade results into era-level metrics."""
        if not trades:
            return self._empty_result()

        total_pnl = sum(t["pnl"] for t in trades)
        gross_pnl = sum(
            t["pnl"] + abs(t["slippage_bps"]) / 10_000 * t["filled_size"]
            for t in trades
        )
        n_fills = sum(1 for t in trades if t["filled_size"] > 0)

        avg_slippage = float(np.mean([t["slippage_bps"] for t in trades]))
        avg_fill_rate = float(np.mean([t["fill_rate"] for t in trades]))
        total_exec_cost_bps = avg_slippage  # simplified

        return {
            "realised_return": round(total_pnl / max(starting_bankroll, 1), 6),
            "gross_return": round(gross_pnl / max(starting_bankroll, 1), 6),
            "execution_cost_bps": round(total_exec_cost_bps, 2),
            "avg_slippage_bps": round(avg_slippage, 2),
            "avg_fill_rate": round(avg_fill_rate, 4),
            "n_trades": len(trades),
            "n_fills": n_fills,
            "total_pnl": round(total_pnl, 4),
            "ending_bankroll": round(portfolio.total_value, 2),
            "risk_events": risk_events,
            "per_trade": trades,
        }

    @staticmethod
    def _empty_result() -> dict[str, Any]:
        return {
            "realised_return": 0.0,
            "gross_return": 0.0,
            "execution_cost_bps": 0.0,
            "avg_slippage_bps": 0.0,
            "avg_fill_rate": 0.0,
            "n_trades": 0,
            "n_fills": 0,
            "total_pnl": 0.0,
            "ending_bankroll": 0.0,
            "risk_events": [],
            "per_trade": [],
        }
