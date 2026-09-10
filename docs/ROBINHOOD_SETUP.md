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

## Wiring into the paper → live chain

The same proof chain as Hyperliquid applies (`.claude/skills/trading-pipeline`):
paper P&L positive, oracle scoring positive, then a wallet row
(`POST /api/v1/trading/wallets` with `exchange: "robinhood"`) and a small live
allocation. The rotation trader can target Robinhood for BTC/ETH/SOL spot the
same way it targets Hyperliquid perps once the account is verified.
