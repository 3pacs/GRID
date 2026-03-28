# Next Agent Instructions

## Agent 1: Paper Trading Auto-Execution Loop (TRADE-01)

### Context
The paper trading engine is built at `trading/paper_engine.py` with tables `paper_trades` and `paper_strategies`. 12 strategies are registered from PASSED TACTICAL hypotheses. Each strategy has a `leader` and `follower` feature, and the signal is: when leader moves >1% in a day, go same direction on follower next day.

The engine has: `open_trade()`, `close_trade()`, `_check_kill()`, `kelly_position_size()`, `get_dashboard()`. What's missing is the **signal detection loop** that monitors features and auto-executes trades.

### What to build

**File:** `/data/grid_v4/grid_repo/grid/trading/signal_executor.py`

```python
"""
Paper Trading Signal Executor.

Runs periodically (hourly during market hours) to:
1. Check all ACTIVE paper strategies for fired signals
2. For each strategy: compare leader's latest daily return to threshold (1%)
3. If signal fires: compute Kelly position size, open paper trade on follower
4. For open trades past their expected_lag days: close at current price
5. Log everything to decision journal for audit trail
"""
```

**Core function: `execute_signals(engine: Engine) -> dict`**

Steps:
1. Load all ACTIVE strategies from `paper_strategies`
2. For each strategy, parse `leader` and `follower` from the linked hypothesis's `lag_structure`
3. Get leader's latest 2 daily prices from `resolved_series` (today and yesterday)
4. Compute daily return: `(today - yesterday) / yesterday`
5. If `abs(return) > 0.01` (1% threshold):
   - Direction: LONG if positive, SHORT if negative
   - Get follower's latest price for entry_price
   - Compute position size via `kelly_position_size` using strategy's historical win_rate and avg_win/avg_loss
   - Call `paper_engine.open_trade()`
6. For open trades where `entry_date + expected_lag <= today`:
   - Get follower's current price
   - Call `paper_engine.close_trade()`
7. Return summary: signals_checked, trades_opened, trades_closed, strategies_killed

**Wire into intelligence loop in `api/main.py`:**
Add to the schedule:
```python
def _paper_trading_signals():
    try:
        from trading.signal_executor import execute_signals
        from db import get_engine as _ge
        result = execute_signals(_ge())
        log.info("Paper trading: {o} opened, {c} closed", o=result.get("trades_opened", 0), c=result.get("trades_closed", 0))
    except Exception as exc:
        log.debug("Paper trading signals failed: {e}", e=str(exc))

# Run every hour during US market hours (14:00-21:00 UTC = 9AM-4PM ET)
_sched.every(1).hours.do(_paper_trading_signals)
```

**Also add API endpoint** in `api/routers/trading.py`:
```python
@router.post("/execute-signals")
async def execute_signals_now(_token: str = Depends(require_auth)) -> dict:
    """Manually trigger signal execution."""
    from trading.signal_executor import execute_signals
    return execute_signals(get_db_engine())
```

### Verification
1. `python -c "from trading.signal_executor import execute_signals; print('OK')"`
2. Run `execute_signals(engine)` and confirm it checks all 12 strategies
3. `pytest tests/ -x -q` passes

---

## Agent 2: Hyperliquid Integration (EXCH-01)

### Context
Hyperliquid is a decentralized perp exchange on Arbitrum. They have a Python SDK (`hyperliquid-python-sdk`). We want testnet first, then mainnet. The crypto backtest winners (BTC→SOL Sharpe 21, ETH→TAO Sharpe 16) are the primary strategies.

### What to build

**Install:** `pip install hyperliquid-python-sdk`

**File:** `/data/grid_v4/grid_repo/grid/trading/hyperliquid.py`

```python
"""
Hyperliquid perp trading integration.

Connects to Hyperliquid DEX for perpetual futures trading.
Testnet first, then mainnet. Uses GRID signals for entry/exit.

Architecture:
  GRID Signal → Position Sizing → Hyperliquid Order → Confirmation → Journal
"""
```

**Class: `HyperliquidTrader`**

Constructor takes:
- `private_key: str` (from env var `HYPERLIQUID_PRIVATE_KEY`)
- `testnet: bool = True` (from env var `HYPERLIQUID_TESTNET=true`)
- `max_position_usd: float = 100.0` (max position size per trade)
- `max_drawdown_pct: float = 0.20` (20% max drawdown per wallet)

Methods:
- `get_balance() -> dict` — wallet balance, margin, open positions
- `get_positions() -> list[dict]` — all open positions with unrealized P&L
- `open_position(ticker: str, direction: str, size_usd: float) -> dict` — market order
- `close_position(ticker: str) -> dict` — close all of a ticker's position
- `get_trade_history(limit=50) -> list[dict]` — recent fills
- `check_risk_limits() -> dict` — is wallet within risk limits?

Add env vars to `config.py`:
```python
HYPERLIQUID_PRIVATE_KEY: str = ""
HYPERLIQUID_TESTNET: bool = True
HYPERLIQUID_MAX_POSITION_USD: float = 100.0
HYPERLIQUID_MAX_DRAWDOWN_PCT: float = 0.20
```

Add env vars to `.env`:
```
HYPERLIQUID_PRIVATE_KEY=
HYPERLIQUID_TESTNET=true
HYPERLIQUID_MAX_POSITION_USD=100
HYPERLIQUID_MAX_DRAWDOWN_PCT=0.20
```

**API endpoints** in `api/routers/trading.py`:
- `GET /trading/hyperliquid/balance` — wallet state
- `GET /trading/hyperliquid/positions` — open positions
- `POST /trading/hyperliquid/trade` — open a position (body: ticker, direction, size_usd)
- `POST /trading/hyperliquid/close` — close a position (body: ticker)

### Verification
1. `python -c "from trading.hyperliquid import HyperliquidTrader; print('OK')"`
2. If SDK installed, test with testnet: `HyperliquidTrader(testnet=True).get_balance()`
3. `pytest tests/ -x -q` passes

---

## Agent 3: Polymarket + Kalshi Integration (EXCH-02, EXCH-03)

### Context
Polymarket is a prediction market (crypto-native, USDC on Polygon). Kalshi is a US-regulated event contract exchange. Both let you bet on binary outcomes (Fed rate decision, CPI print, election results, etc.). GRID's regime analysis + macro data gives an edge.

### What to build

**Install:** `pip install py-clob-client` (Polymarket), Kalshi has a REST API

**File:** `/data/grid_v4/grid_repo/grid/trading/prediction_markets.py`

Two classes:

**`PolymarketTrader`:**
- Constructor: `api_key` from env `POLYMARKET_API_KEY`, `private_key` from `POLYMARKET_PRIVATE_KEY`
- `get_markets(query: str) -> list[dict]` — search active markets
- `get_position(market_id: str) -> dict` — current position + P&L
- `buy(market_id: str, outcome: str, amount_usd: float) -> dict` — buy shares
- `sell(market_id: str, outcome: str, amount: float) -> dict` — sell shares
- `get_portfolio() -> dict` — all positions

**`KalshiTrader`:**
- Constructor: `email` from env `KALSHI_EMAIL`, `password` from `KALSHI_PASSWORD`
- `get_events(category: str = None) -> list[dict]` — active event contracts
- `get_position(event_id: str) -> dict` — current position
- `buy(event_id: str, side: str, contracts: int, price_cents: int) -> dict`
- `sell(event_id: str, contracts: int, price_cents: int) -> dict`
- `get_portfolio() -> dict`

Add env vars to config.py and .env.

**API endpoints** in `api/routers/trading.py`:
- `GET /trading/polymarket/markets?query=` — search markets
- `GET /trading/polymarket/portfolio` — positions
- `GET /trading/kalshi/events` — active events
- `GET /trading/kalshi/portfolio` — positions

### Verification
1. Import test
2. `pytest tests/ -x -q` passes

---

## Agent 4: Multi-Wallet Manager (EXCH-04)

### What to build

**File:** `/data/grid_v4/grid_repo/grid/trading/wallet_manager.py`

**DB table:** `trading_wallets`
```sql
CREATE TABLE IF NOT EXISTS trading_wallets (
    id              TEXT PRIMARY KEY,
    exchange        TEXT NOT NULL,
    wallet_type     TEXT NOT NULL DEFAULT 'paper',
    initial_capital FLOAT NOT NULL,
    current_capital FLOAT NOT NULL,
    high_water_mark FLOAT NOT NULL,
    max_drawdown    FLOAT NOT NULL DEFAULT 0,
    total_pnl       FLOAT NOT NULL DEFAULT 0,
    total_trades    INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'ACTIVE',
    risk_limit_pct  FLOAT NOT NULL DEFAULT 0.05,
    max_drawdown_limit FLOAT NOT NULL DEFAULT 0.20,
    metadata        JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
```

**Class: `WalletManager`**
- `create_wallet(exchange, wallet_type, initial_capital, risk_limits) -> str`
- `get_wallet(wallet_id) -> dict`
- `update_pnl(wallet_id, pnl) -> dict` — updates capital, HWM, drawdown, checks limits
- `get_all_wallets() -> list[dict]`
- `get_dashboard() -> dict` — aggregated across all wallets
- `check_risk(wallet_id) -> dict` — is this wallet still within limits?
- `kill_wallet(wallet_id, reason) -> dict`

**API endpoints:**
- `GET /trading/wallets` — all wallets
- `POST /trading/wallets` — create wallet
- `GET /trading/wallets/{id}` — single wallet
- `POST /trading/wallets/{id}/kill` — kill wallet

### Verification
1. Import + create wallet test
2. Tests pass
