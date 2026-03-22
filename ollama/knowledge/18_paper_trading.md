# Paper Trading — Falsifiable Prediction Tracking

## Overview

GRID's paper trading system creates timestamped, immutable snapshots of regime
calls with specific, falsifiable predictions. After predictions expire, they are
scored against actual market data. This builds a verifiable track record.

## How It Works

### 1. Create Snapshot

Captures current state:
- Current regime (CRISIS, FRAGILE, GROWTH, NEUTRAL)
- Regime confidence (0.0–1.0)
- Feature count and transition probability
- Timestamp (immutable)

### 2. Generate Predictions

Regime-specific predictions with confidence scaling:

| Regime | Predictions |
|--------|------------|
| CRISIS | SPY down, TLT up (flight to safety), GLD up, VIX stays elevated |
| FRAGILE | SPY flat/down, GLD outperforms SPY |
| GROWTH | SPY up, BTC up (risk-on proxy) |
| NEUTRAL | SPY flat (range-bound, low volatility) |

Each prediction includes:
- **Asset**: What's being predicted (SPY, TLT, GLD, VIX, BTC)
- **Direction**: UP, DOWN, or FLAT
- **Horizon**: Days until evaluation (default: 5, 10, 21 days)
- **Confidence**: Scaled by regime confidence
- **Description**: Human-readable reasoning

### 3. Save Snapshot

- Saved as immutable JSON to `outputs/paper_trades/snapshot_YYYYMMDD_HHMMSS.json`
- Also logged to decision_journal with `action_taken = "PAPER_TRADE"`
- Cannot be modified after creation

### 4. Score Predictions

After the horizon expires:
- Fetch actual returns for each predicted asset
- Compare direction (UP/DOWN/FLAT) against actual movement
- Score: correct/incorrect/inconclusive
- Aggregate accuracy metrics

## CLI Usage

```bash
# Create a new snapshot
python -m backtest.paper_trade --snapshot

# List all snapshots
python -m backtest.paper_trade --list

# Score expired predictions
python -m backtest.paper_trade --score
```

## API Endpoints

- POST `/api/v1/backtest/paper-trade` — Create snapshot
- POST `/api/v1/backtest/paper-trade/score` — Score expired predictions

## Why This Matters

Paper trading is the bridge between backtesting and live trading:
- **Backtests** use historical data (risk of overfitting)
- **Paper trades** use real-time predictions (no hindsight bias)
- **Track record** builds confidence before risking capital
- **Immutability** prevents cherry-picking or retrospective editing

## Key Files

- `backtest/paper_trade.py` — PaperTrader class
- `api/routers/backtest.py` — REST endpoints
