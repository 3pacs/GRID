# Paper Trading

Run the rotation paper trader, check positions, or show dashboard.

## Arguments

- `dashboard` — show current positions and P&L
- `run` — execute one rotation cycle (default)

## Instructions

### Show Dashboard
```bash
cd /data/grid_v4/grid_repo && python3 scripts/rotation_paper_trader.py --dashboard
```

### Run One Cycle
```bash
cd /data/grid_v4/grid_repo && python3 scripts/rotation_paper_trader.py
```

### Check Live Hyperliquid Status
```bash
cd /data/grid_v4/grid_repo && python3 scripts/live_rotation_trader.py --status
```

## How It Works

1. `run_rotation()` computes current regime + target weights
2. Compares to open paper_trades for strategy `adaptive_rotation_live`
3. Closes positions not in target or stopped
4. Opens new positions matching target allocation
5. Tracks P&L via wallet_manager (wallet: `grid_rotation_paper_*`)

## Regime → Allocation

| Regime | Equity Groups | Cash Floor | Max Groups |
|--------|--------------|------------|------------|
| risk-on | growth_tech + real_assets | 0% | 2 |
| neutral | real_assets + defensive | 20% | 2 |
| risk-off | defensive only | 50% | 1 |

## Regime → Crypto (Hyperliquid)

| Regime | Allocation |
|--------|-----------|
| risk-on | 60% BTC, 25% ETH, 15% SOL |
| neutral | 50% BTC, 50% cash |
| risk-off | 100% cash |

## Key Files
- `scripts/rotation_paper_trader.py` — paper trading runner
- `scripts/live_rotation_trader.py` — Hyperliquid live trader
- `trading/paper_engine.py` — paper trade engine
- `trading/wallet_manager.py` — wallet P&L tracking
- `alpha_research/strategies/adaptive_rotation.py` — rotation strategy
- Hermes step 7g runs this daily at 17:00-17:30 UTC
