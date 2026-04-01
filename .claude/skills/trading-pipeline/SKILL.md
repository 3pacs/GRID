# trading-pipeline

End-to-end trading pipeline from alpha signals to live execution. Covers paper trading, live Hyperliquid integration, position sizing, risk controls, and the proof chain to live money.

## When to Use This Skill

- Setting up new trading strategies
- Debugging paper trading P&L
- Moving from paper to live trading
- Understanding the regime-to-allocation mapping
- Configuring risk controls and kill switches

## Architecture

```
Alpha Signals → Rotation Strategy → Regime Detection → Allocation Weights
    ↓                                                        ↓
Paper Trades ← Signal Executor ← Kelly Sizing ← Position Manager
    ↓                                                        ↓
Wallet Manager ← P&L Tracking ← Risk Limits ← Kill Switch
    ↓
Live Execution (Hyperliquid) ← when paper P&L proves positive
```

## Components

### Adaptive Rotation Strategy
- File: `alpha_research/strategies/adaptive_rotation.py`
- Backtest: Sharpe 1.56, +83% vs SPY +59% (2.5 years)
- Regime: 26-week SPY trend + VIX z-score → risk-on/neutral/risk-off
- Fast risk-off: 3-day SPY drawdown < -3% OR VIX z > 3.0
- Asset groups: growth_tech, real_assets, defensive (max 2 active)
- Risk: 5% absolute stop, 10% trailing stop, 20-day cooldown

### Paper Trading Engine
- File: `trading/paper_engine.py`
- Tables: paper_trades, paper_strategies
- Auto-kill: win_rate < 40% OR drawdown > 5% after 20 trades
- Kelly criterion position sizing (half-Kelly, capped at 25%)

### Wallet Manager
- File: `trading/wallet_manager.py`
- Table: trading_wallets
- Per-wallet: capital tracking, HWM, drawdown, auto-kill at 20% DD
- Multi-exchange support (grid_rotation, hyperliquid, etc.)

### Signal Executor
- File: `trading/signal_executor.py`
- Runs hourly during market hours
- Checks leader return vs 1% threshold → fires signal → opens paper trade
- Auto-closes after expected_lag days
- Convergence boost: trust_scorer detects 3+ sources agreeing → 1.2-1.5x size

### Hyperliquid Live Trader
- File: `scripts/live_rotation_trader.py`
- Maps regime → crypto allocation (BTC/ETH/SOL)
- Risk controls: $100 max position, 20% max drawdown
- Wallet: 0x7A7a183843FfCfDCcDF27dcEa095aE89dD31adAA
- SDK: hyperliquid-python-sdk 0.22.0

### Rotation Paper Trader
- File: `scripts/rotation_paper_trader.py`
- Daily runner (Hermes step 7g, 17:00-17:30 UTC)
- Compares target weights to current positions
- Opens/closes to match allocation
- Strategy ID: `adaptive_rotation_live`

## Proof Chain to Live Money

```
1. Ensemble val Sharpe > 0.5    ✓ (0.37 — close, needs volume data)
2. Rotation backtest Sharpe > 1 ✓ (1.56)
3. Paper trading P&L positive   ⏳ (positions opened 2026-03-31)
4. Oracle scoring positive       ⏳ (starts Apr 5)
5. Fund Hyperliquid wallet       ⏳ (after step 3+4)
6. Live $100 on mainnet          ⏳
```

## Risk Controls Summary

| Layer | Mechanism | Threshold |
|-------|-----------|-----------|
| Strategy | Absolute stop | 5% from entry |
| Strategy | Trailing stop | 10% from peak |
| Strategy | Cooldown | 20 days after stop |
| Paper Engine | Auto-kill | win_rate < 40% OR DD > 5% |
| Wallet Manager | Auto-kill | DD > 20% |
| Hyperliquid | Max position | $100 USD |
| Hyperliquid | Max drawdown | 20% |
| Circuit Breaker | Halt | 3 consecutive failures |
