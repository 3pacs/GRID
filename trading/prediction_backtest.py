"""
GRID Prediction Market Backtesting Bridge.

Exports GRID's historical prediction market data to Parquet format
compatible with evan-kolberg/prediction-market-backtesting (NautilusTrader).

Also provides a hypothesis runner that lets you define and test prediction
market strategies against the historical dataset.

Usage:
    from trading.prediction_backtest import (
        export_kalshi_trades,
        export_polymarket_trades,
        run_hypothesis,
    )

    # Export data for the backtester
    export_kalshi_trades(engine, "/path/to/output")
    export_polymarket_trades(engine, "/path/to/output")

    # Run a hypothesis
    result = run_hypothesis(
        engine,
        name="fed_rate_momentum",
        market_filter="fed-rate%",
        strategy="momentum_reversal",
        params={"lookback": 24, "threshold": 0.05},
    )
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ── Output paths ────────────────────────────────────────────────────
_DEFAULT_EXPORT_DIR = Path(
    os.environ.get(
        "PM_BACKTEST_EXPORT_DIR",
        Path(__file__).resolve().parents[1] / "data" / "prediction_markets" / "backtest_export",
    )
)


@dataclass
class HypothesisResult:
    """Result of a prediction market hypothesis backtest."""

    name: str
    description: str
    market_filter: str
    strategy: str
    params: dict[str, Any]
    start_date: str
    end_date: str
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_pnl: float = 0.0
    sharpe_ratio: float | None = None
    max_drawdown: float | None = None
    brier_score: float | None = None
    win_rate: float | None = None
    avg_edge: float | None = None
    daily_returns: list[float] = field(default_factory=list)
    trade_log: list[dict[str, Any]] = field(default_factory=list)

    @property
    def summary(self) -> str:
        wr = f"{self.win_rate:.1%}" if self.win_rate else "N/A"
        return (
            f"Hypothesis: {self.name}\n"
            f"  Period: {self.start_date} → {self.end_date}\n"
            f"  Trades: {self.total_trades} (W:{self.winning_trades} L:{self.losing_trades})\n"
            f"  Win Rate: {wr}\n"
            f"  PnL: ${self.total_pnl:,.2f}\n"
            f"  Sharpe: {self.sharpe_ratio:.2f}" if self.sharpe_ratio else ""
            f"  Max DD: {self.max_drawdown:.1%}" if self.max_drawdown else ""
        )


# ── Data Export ─────────────────────────────────────────────────────


def export_kalshi_trades(
    engine: Engine,
    output_dir: str | Path | None = None,
    market_filter: str | None = None,
) -> Path:
    """Export Kalshi trades from GRID DB to Parquet for the backtester.

    Args:
        engine: SQLAlchemy engine.
        output_dir: Output directory (default: data/prediction_markets/backtest_export).
        market_filter: SQL LIKE pattern to filter markets (e.g., 'fed-rate%').

    Returns:
        Path to the exported Parquet file.
    """
    out = Path(output_dir or _DEFAULT_EXPORT_DIR) / "kalshi"
    out.mkdir(parents=True, exist_ok=True)

    query = """
        SELECT
            t.market_id AS ticker,
            t.trade_id,
            t.trade_timestamp AS ts,
            t.price AS yes_price,
            (1.0 - t.price) AS no_price,
            t.size AS count,
            t.taker_side,
            m.title,
            m.category,
            m.status AS market_status
        FROM prediction_market_trades t
        LEFT JOIN prediction_market_markets m
            ON m.platform = t.platform AND m.market_id = t.market_id
        WHERE t.platform = 'kalshi'
    """
    params: dict[str, Any] = {}

    if market_filter:
        query += " AND t.market_id LIKE :mf"
        params["mf"] = market_filter

    query += " ORDER BY t.trade_timestamp"

    log.info("Exporting Kalshi trades...")
    df = pd.read_sql(text(query), engine, params=params)
    outfile = out / "trades.parquet"
    df.to_parquet(outfile, index=False)
    log.info("Exported {n} Kalshi trades to {f}", n=len(df), f=outfile)
    return outfile


def export_polymarket_trades(
    engine: Engine,
    output_dir: str | Path | None = None,
    market_filter: str | None = None,
) -> Path:
    """Export Polymarket trades from GRID DB to Parquet for the backtester."""
    out = Path(output_dir or _DEFAULT_EXPORT_DIR) / "polymarket"
    out.mkdir(parents=True, exist_ok=True)

    query = """
        SELECT
            t.market_id,
            t.trade_id AS id,
            t.trade_timestamp AS timestamp,
            t.price,
            t.size,
            t.side,
            t.maker_address AS maker,
            t.taker_address AS taker,
            t.fee,
            t.tx_hash AS "transactionHash",
            t.block_number AS "blockNumber",
            m.title AS question,
            m.outcomes,
            m.category
        FROM prediction_market_trades t
        LEFT JOIN prediction_market_markets m
            ON m.platform = t.platform AND m.market_id = t.market_id
        WHERE t.platform = 'polymarket'
    """
    params: dict[str, Any] = {}

    if market_filter:
        query += " AND t.market_id LIKE :mf"
        params["mf"] = market_filter

    query += " ORDER BY t.trade_timestamp"

    log.info("Exporting Polymarket trades...")
    df = pd.read_sql(text(query), engine, params=params)
    outfile = out / "trades.parquet"
    df.to_parquet(outfile, index=False)
    log.info("Exported {n} Polymarket trades to {f}", n=len(df), f=outfile)
    return outfile


def export_markets(
    engine: Engine,
    platform: str = "kalshi",
    output_dir: str | Path | None = None,
) -> Path:
    """Export market metadata to Parquet."""
    out = Path(output_dir or _DEFAULT_EXPORT_DIR) / platform
    out.mkdir(parents=True, exist_ok=True)

    df = pd.read_sql(
        text("""
            SELECT * FROM prediction_market_markets
            WHERE platform = :p
            ORDER BY created_at
        """),
        engine,
        params={"p": platform},
    )
    outfile = out / "markets.parquet"
    df.to_parquet(outfile, index=False)
    log.info("Exported {n} {p} markets to {f}", n=len(df), p=platform, f=outfile)
    return outfile


# ── Hypothesis Testing ──────────────────────────────────────────────

# Built-in strategies
_STRATEGIES: dict[str, type] = {}


def register_strategy(name: str):
    """Decorator to register a backtest strategy."""
    def decorator(cls):
        _STRATEGIES[name] = cls
        return cls
    return decorator


class BaseStrategy:
    """Base class for prediction market hypothesis strategies."""

    def __init__(self, params: dict[str, Any] | None = None):
        self.params = params or {}
        self.position: float = 0.0
        self.cash: float = 10000.0
        self.trades: list[dict[str, Any]] = []

    def on_trade(self, trade: pd.Series, market_state: dict[str, Any]) -> str | None:
        """Process a trade and return action: 'buy_yes', 'buy_no', 'close', or None."""
        raise NotImplementedError

    def on_market_close(self, market_id: str, outcome: str) -> float:
        """Called when market resolves. Returns PnL."""
        if self.position > 0 and outcome == "yes":
            return self.position * 1.0 - self.position * self._avg_entry
        elif self.position > 0 and outcome == "no":
            return -self.position * self._avg_entry
        return 0.0


@register_strategy("momentum_reversal")
class MomentumReversalStrategy(BaseStrategy):
    """Buy when price drops sharply (mean reversion hypothesis).

    Hypothesis: Prediction market prices overreact to news, creating
    short-term mispricings. When a market moves >threshold in lookback
    hours, take the contrarian position.
    """

    def on_trade(self, trade: pd.Series, market_state: dict[str, Any]) -> str | None:
        self.params.get("lookback", 24)
        threshold = self.params.get("threshold", 0.05)
        recent = market_state.get("recent_prices", [])

        if len(recent) < 2:
            return None

        price_change = recent[-1] - recent[0]

        # Contrarian: buy YES when price dropped, buy NO when price spiked
        if price_change < -threshold and self.position == 0:
            return "buy_yes"
        elif price_change > threshold and self.position == 0:
            return "buy_no"
        return None


@register_strategy("maker_flow")
class MakerFlowStrategy(BaseStrategy):
    """Follow sophisticated maker flow (Becker's wealth transfer thesis).

    Hypothesis: Makers profit by accommodating biased taker flow.
    When taker flow is heavily one-sided, fade it like a maker would.
    """

    def on_trade(self, trade: pd.Series, market_state: dict[str, Any]) -> str | None:
        window = self.params.get("window", 50)
        imbalance_threshold = self.params.get("imbalance_threshold", 0.7)
        recent_sides = market_state.get("recent_taker_sides", [])

        if len(recent_sides) < window:
            return None

        # Count taker side imbalance
        yes_count = sum(1 for s in recent_sides[-window:] if s in ("yes", "buy"))
        imbalance = yes_count / window

        if imbalance > imbalance_threshold and self.position == 0:
            return "buy_no"  # Fade the crowd
        elif imbalance < (1 - imbalance_threshold) and self.position == 0:
            return "buy_yes"  # Fade the crowd
        return None


@register_strategy("value_divergence")
class ValueDivergenceStrategy(BaseStrategy):
    """Buy when market price diverges from fundamental signals.

    Hypothesis: When GRID's other data sources (congressional trades,
    insider filings, macro data) suggest a different probability than
    the market price, the market will converge to fundamentals.
    """

    def on_trade(self, trade: pd.Series, market_state: dict[str, Any]) -> str | None:
        min_divergence = self.params.get("min_divergence", 0.15)
        grid_estimate = market_state.get("grid_probability_estimate")

        if grid_estimate is None:
            return None

        market_price = trade.get("price", 0.5)
        divergence = grid_estimate - market_price

        if divergence > min_divergence and self.position == 0:
            return "buy_yes"
        elif divergence < -min_divergence and self.position == 0:
            return "buy_no"
        return None


@register_strategy("liquidity_spike")
class LiquiditySpikeStrategy(BaseStrategy):
    """Trade around liquidity spikes (unusual volume = informed flow).

    Hypothesis: Sudden volume spikes in prediction markets indicate
    informed traders acting on non-public information.
    """

    def on_trade(self, trade: pd.Series, market_state: dict[str, Any]) -> str | None:
        vol_multiple = self.params.get("vol_multiple", 3.0)
        recent_volumes = market_state.get("recent_volumes", [])

        if len(recent_volumes) < 20:
            return None

        avg_vol = sum(recent_volumes[-20:]) / 20
        current_vol = trade.get("size", 0)

        if avg_vol > 0 and current_vol > avg_vol * vol_multiple:
            # Follow the spike direction
            side = trade.get("taker_side", trade.get("side", ""))
            if side in ("yes", "buy") and self.position == 0:
                return "buy_yes"
            elif side in ("no", "sell") and self.position == 0:
                return "buy_no"
        return None


def run_hypothesis(
    engine: Engine,
    name: str,
    market_filter: str,
    strategy: str,
    params: dict[str, Any] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    position_size: float = 100.0,
    description: str = "",
) -> HypothesisResult:
    """Run a prediction market hypothesis backtest against GRID data.

    Args:
        engine: SQLAlchemy engine.
        name: Hypothesis name for logging.
        market_filter: SQL LIKE pattern for market_id.
        strategy: Strategy name (registered via @register_strategy).
        params: Strategy-specific parameters.
        start_date: ISO date string for backtest start.
        end_date: ISO date string for backtest end.
        position_size: USD per trade.
        description: Human-readable hypothesis description.

    Returns:
        HypothesisResult with full backtest metrics.
    """
    if strategy not in _STRATEGIES:
        raise ValueError(
            f"Unknown strategy '{strategy}'. "
            f"Available: {list(_STRATEGIES.keys())}"
        )

    strat = _STRATEGIES[strategy](params or {})
    strat.cash = position_size * 100  # Starting capital

    # Fetch trades
    query = """
        SELECT
            t.platform, t.market_id, t.trade_id,
            t.trade_timestamp, t.price, t.size,
            t.side, t.taker_side
        FROM prediction_market_trades t
        WHERE t.market_id LIKE :mf
          AND t.price IS NOT NULL
          AND t.price BETWEEN 0 AND 1
    """
    bind: dict[str, Any] = {"mf": market_filter}

    if start_date:
        query += " AND t.trade_timestamp >= :sd"
        bind["sd"] = start_date
    if end_date:
        query += " AND t.trade_timestamp <= :ed"
        bind["ed"] = end_date

    query += " ORDER BY t.trade_timestamp"

    log.info(
        "Running hypothesis '{n}' with strategy '{s}' on markets '{m}'",
        n=name, s=strategy, m=market_filter,
    )

    df = pd.read_sql(text(query), engine, params=bind)

    if df.empty:
        log.warning("No trades found for filter '{m}'", m=market_filter)
        return HypothesisResult(
            name=name, description=description,
            market_filter=market_filter, strategy=strategy,
            params=params or {}, start_date=start_date or "",
            end_date=end_date or "",
        )

    # Build market state tracking
    market_states: dict[str, dict[str, Any]] = {}
    trade_log: list[dict[str, Any]] = []
    daily_pnl: dict[str, float] = {}
    total_pnl = 0.0
    wins = 0
    losses = 0

    for _, trade in df.iterrows():
        mid = trade["market_id"]

        # Initialize market state
        if mid not in market_states:
            market_states[mid] = {
                "recent_prices": [],
                "recent_volumes": [],
                "recent_taker_sides": [],
                "entry_price": None,
                "position": 0,
                "position_side": None,
            }

        ms = market_states[mid]
        price = float(trade["price"])

        # Update rolling state
        ms["recent_prices"].append(price)
        ms["recent_volumes"].append(float(trade.get("size", 0) or 0))
        ms["recent_taker_sides"].append(str(trade.get("taker_side", "")))

        # Keep rolling windows bounded
        for key in ["recent_prices", "recent_volumes", "recent_taker_sides"]:
            if len(ms[key]) > 200:
                ms[key] = ms[key][-200:]

        # Run strategy
        action = strat.on_trade(trade, ms)
        ts = trade["trade_timestamp"]
        day = str(ts)[:10] if ts else "unknown"

        if action == "buy_yes" and ms["position"] == 0:
            ms["position"] = position_size
            ms["position_side"] = "yes"
            ms["entry_price"] = price
            trade_log.append({
                "ts": str(ts), "market": mid, "action": "BUY_YES",
                "price": price, "size": position_size,
            })

        elif action == "buy_no" and ms["position"] == 0:
            ms["position"] = position_size
            ms["position_side"] = "no"
            ms["entry_price"] = 1.0 - price
            trade_log.append({
                "ts": str(ts), "market": mid, "action": "BUY_NO",
                "price": 1.0 - price, "size": position_size,
            })

        elif action == "close" and ms["position"] > 0:
            exit_price = price if ms["position_side"] == "yes" else 1.0 - price
            pnl = (exit_price - ms["entry_price"]) * ms["position"]
            total_pnl += pnl

            if pnl > 0:
                wins += 1
            else:
                losses += 1

            daily_pnl[day] = daily_pnl.get(day, 0) + pnl
            trade_log.append({
                "ts": str(ts), "market": mid, "action": "CLOSE",
                "price": exit_price, "pnl": pnl,
            })

            ms["position"] = 0
            ms["position_side"] = None
            ms["entry_price"] = None

    # Calculate metrics
    total_trades = wins + losses
    daily_returns = list(daily_pnl.values())

    sharpe = None
    if daily_returns and len(daily_returns) > 1:
        import statistics
        avg_ret = statistics.mean(daily_returns)
        std_ret = statistics.stdev(daily_returns)
        if std_ret > 0:
            sharpe = (avg_ret / std_ret) * (252 ** 0.5)

    max_dd = None
    if daily_returns:
        cumulative = 0.0
        peak = 0.0
        worst_dd = 0.0
        for r in daily_returns:
            cumulative += r
            if cumulative > peak:
                peak = cumulative
            dd = (peak - cumulative) / max(peak, 1) if peak > 0 else 0
            if dd > worst_dd:
                worst_dd = dd
        max_dd = worst_dd

    result = HypothesisResult(
        name=name,
        description=description,
        market_filter=market_filter,
        strategy=strategy,
        params=params or {},
        start_date=str(df["trade_timestamp"].min())[:10] if not df.empty else "",
        end_date=str(df["trade_timestamp"].max())[:10] if not df.empty else "",
        total_trades=total_trades,
        winning_trades=wins,
        losing_trades=losses,
        total_pnl=total_pnl,
        sharpe_ratio=sharpe,
        max_drawdown=max_dd,
        win_rate=wins / total_trades if total_trades > 0 else None,
        daily_returns=daily_returns,
        trade_log=trade_log,
    )

    log.info(
        "Hypothesis '{n}': {t} trades, PnL=${p:,.2f}, Win={w}",
        n=name, t=total_trades, p=total_pnl,
        w=f"{result.win_rate:.1%}" if result.win_rate else "N/A",
    )
    return result


def list_strategies() -> list[dict[str, str]]:
    """List all registered backtest strategies."""
    results = []
    for name, cls in _STRATEGIES.items():
        results.append({
            "name": name,
            "description": (cls.__doc__ or "").strip().split("\n")[0],
        })
    return results


def list_available_markets(
    engine: Engine,
    platform: str | None = None,
    search: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    """List available markets for backtesting."""
    query = """
        SELECT
            platform, market_id, title, category, status,
            volume, created_at, closed_at
        FROM prediction_market_markets
        WHERE 1=1
    """
    params: dict[str, Any] = {}

    if platform:
        query += " AND platform = :p"
        params["p"] = platform

    if search:
        query += " AND (title ILIKE :s OR market_id ILIKE :s OR category ILIKE :s)"
        params["s"] = f"%{search}%"

    query += " ORDER BY volume DESC NULLS LAST LIMIT :lim"
    params["lim"] = limit

    return pd.read_sql(text(query), engine, params=params)
