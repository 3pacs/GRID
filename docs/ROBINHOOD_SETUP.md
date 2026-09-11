# Robinhood connector — account set-up

GRID's Robinhood connector (`trading/robinhood.py`) uses Robinhood's **official
Crypto Trading API**: an API key plus an Ed25519 signature on every request.
It is crypto-spot only — Robinhood publishes no equities API, and the connector
never touches the brokerage login or MFA. Orders stay in **dry-run** until
`ROBINHOOD_LIVE_TRADING=true`.

## 1. Generate the keypair (on grid-svr, as the `grid` user)

```bash
cd /data/grid_v4/grid_release
python3 -m trading.robinhood keygen
```

It prints two base64 strings. The **private** key goes into the server `.env`
only. The **public** key is what Robinhood asks for.

## 2. Create the API credential in Robinhood

Robinhood app → Account (person icon) → **API** (under "Crypto") → *Add API
key*. Paste the public key, pick the permissions GRID needs (read account,
read market data, place and cancel crypto orders), and copy the API key
Robinhood issues. Note the expiry Robinhood assigns — it has to be renewed
before that date.

## 3. Put the credentials on the server

Edit `/home/grid/grid_v4/grid_repo/.env` (the release tree reads the same
file) and set:

```
ROBINHOOD_API_KEY=<key issued by Robinhood>
ROBINHOOD_PRIVATE_KEY_B64=<private key from step 1>
ROBINHOOD_LIVE_TRADING=false
ROBINHOOD_MAX_POSITION_USD=100
ROBINHOOD_MAX_DRAWDOWN_PCT=0.20
```

Restart `grid-api`, then confirm:

```bash
python3 -m trading.robinhood status          # mode DRY_RUN, account …1234
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/v1/trading/robinhood/status
```

## 4. Dry-run an order

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"ticker":"BTC","direction":"LONG","size_usd":25}' \
  http://localhost:8000/api/v1/trading/robinhood/trade
```

The response carries `status: dry_run` with the exact order that would have
been sent (symbol, quantity rounded to the pair's increment, reference price).
Nothing reaches Robinhood.

## 5. Go live

Set `ROBINHOOD_LIVE_TRADING=true`, restart `grid-api`, and repeat step 4 with a
small size. Risk rails that stay on regardless:

| Rail | Setting | Default |
|---|---|---|
| Per-order notional cap | `ROBINHOOD_MAX_POSITION_USD` | $100 |
| Equity drawdown halt | `ROBINHOOD_MAX_DRAWDOWN_PCT` | 20 % from the high-water mark |
| Shorts | n/a | spot only — SHORT sells held quantity, never goes net short |

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/v1/trading/robinhood/status` | mode, caps, account |
| GET | `/api/v1/trading/robinhood/balance` | buying power, holdings value, equity, HWM |
| GET | `/api/v1/trading/robinhood/positions` | holdings valued at mid |
| GET | `/api/v1/trading/robinhood/orders?limit=50` | recent orders |
| POST | `/api/v1/trading/robinhood/trade` | `{ticker, direction, size_usd}` market order |
| POST | `/api/v1/trading/robinhood/close` | `{ticker}` sell the whole holding |
| POST | `/api/v1/trading/robinhood/orders/{id}/cancel` | cancel an open order |

All endpoints require the normal GRID bearer token.

`GET /api/v1/system/health` carries a `checks.robinhood` block (mode,
configured, live_trading, caps). It reads local config only — health never
calls Robinhood — and reports `mode: ERROR` plus a degraded reason when the
key in `.env` cannot be loaded.

## Wiring into the paper → live chain

The same proof chain as Hyperliquid applies (`.claude/skills/trading-pipeline`):
paper P&L positive, oracle scoring positive, then a small live allocation.
Every step below stays dry-run until `ROBINHOOD_LIVE_TRADING=true`.

### 1. Tracking wallet

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"exchange":"robinhood","wallet_type":"live","initial_capital":100,
       "risk_limit_pct":0.05,"max_drawdown_limit":0.20}' \
  http://localhost:8000/api/v1/trading/wallets
```

`trading/wallet_manager.py` then tracks P&L, high-water mark and drawdown for
that pool and auto-kills it at 20 %. It shows up in
`GET /api/v1/trading/wallets/dashboard` under `per_exchange.robinhood`.

### 2. Rotation trader

```bash
python3 scripts/live_rotation_trader.py --venue robinhood --status
python3 scripts/live_rotation_trader.py --venue robinhood
```

Same regime → target-weight map as Hyperliquid, executed as **spot**: long
only, risk-off sells everything to cash, and a rebalance trades the delta
(buy up, or sell part of the holding) instead of closing and re-opening.
Target coins Robinhood does not list as tradable are dropped and stay in cash.
If the pair lookup comes back empty the cycle is skipped (`status:
VENUE_UNAVAILABLE`) with positions untouched — an API blip must not be read as
risk-off and liquidate the book. Hyperliquid remains the default venue; Hermes
step 7g stays paper.

### 3. Signal executor venue tag

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/trading/execute-signals?venue=robinhood&wallet_id=<wallet>"
```

The paper trade is opened exactly as before; on top of it a LONG signal whose
follower resolves to a tradable crypto pair is sent to Robinhood, sized as
`wallet capital × Kelly fraction` and capped by `ROBINHOOD_MAX_POSITION_USD`.
SHORT signals, non-crypto tickers, dust orders and non-ACTIVE wallets route
nothing. The run summary carries `venue`, `venue_mode` and `venue_orders`.
