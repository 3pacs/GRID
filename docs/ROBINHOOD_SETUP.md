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
small size. Risk rails that stay on regardless — every persisted one lives in
`trading_risk_state` / `trading_order_log` (see "Persisted state" below), not
on the connector object, so they survive a restart and the fresh
`RobinhoodCryptoTrader` instance `get_robinhood_trader()` builds on every
call:

| Rail | Setting | Default | Persisted? |
|---|---|---|---|
| Per-order notional cap | `ROBINHOOD_MAX_POSITION_USD` | $100 | no (a pure input check) |
| Equity drawdown halt | `ROBINHOOD_MAX_DRAWDOWN_PCT` | 20% from the high-water mark | yes |
| Daily loss cap (blocks new buys; sells/closes stay allowed) | `ROBINHOOD_MAX_DAILY_LOSS_PCT` | 5% from start-of-day equity | yes |
| Order-rate cap (blocks new buys) | `ROBINHOOD_MAX_ORDERS_PER_DAY` | 6/day | yes |
| Idempotent submission | `client_order_id` / API `idempotency_key` | n/a | yes — repeat comes back `status="duplicate"` |
| Stale-quote guard | `ROBINHOOD_MAX_QUOTE_AGE_S` | 30s | n/a (checked fresh every call) |
| Spread guard | `ROBINHOOD_MAX_SPREAD_BPS` | 50bps | n/a |
| Marketable limit orders | `ROBINHOOD_USE_LIMIT_ORDERS` / `ROBINHOOD_LIMIT_SLIPPAGE_BPS` | on / 25bps | n/a |
| Wallet gate (KILLED/PAUSED/no wallet blocks orders) | n/a — an ACTIVE `trading_wallets` row for `exchange='robinhood'` | n/a | reads `trading_wallets` |
| Shorts | n/a | spot only — SHORT sells held quantity, never goes net short | n/a |

The 50bps spread cap is a deliberately loose ceiling: BTC/ETH on Robinhood
typically trade inside 5-10bps, so 50bps only fires on a genuinely
dislocated or illiquid quote while leaving headroom for smaller pairs GRID
might route later.

### Persisted state

`trading_risk_state` (one row per venue) holds the drawdown high-water mark,
start-of-day equity and today's order count; `trading_order_log` is the
idempotent, cost-accounting audit trail (bid/ask/mid, the executable price,
spread cost, and — once known — the fill price for every attempt that got
far enough to be built). Both are created **only** by
`migrations/versions/robinhood_guards_20260924.py`, applied by the normal
`alembic upgrade head` every deploy already runs. `RobinhoodCryptoTrader`
never touches SQL directly — it talks to a `RiskStore`
(`trading/robinhood_risk_store.py`): `PostgresRiskStore` in production,
`InMemoryRiskStore` (process-local, resets per instance — the old behavior)
for anything that doesn't wire a database in, e.g. tests and ad-hoc scripts.

**`PostgresRiskStore` does not create these tables at runtime.** If the
migration hasn't been applied, every order fails closed with an unhandled
`sqlalchemy.exc.ProgrammingError` (undefined table) rather than silently
bootstrapping a schema under the API's own role — deliberately, since a
table created ad hoc that way can end up with the wrong owner/grants (see
the GRANT-footer convention in `migrations/_TEMPLATE.sql`). Confirm the
migration has actually run (`alembic current` on the server, or
`SELECT 1 FROM trading_risk_state LIMIT 1` via read-only psql) before
expecting any Robinhood order — dry-run or live — to succeed.

### Wallet gate

Every order (open and close, on all three paths: the API routes,
`scripts/live_rotation_trader.py --venue robinhood`, and
`trading/signal_executor.py`'s `VenueTag`) requires an ACTIVE
`trading_wallets` row for `exchange='robinhood'` — see "Tracking wallet"
below to create one. A KILLED or PAUSED wallet, or none at all, blocks
every order with `status="blocked", guard="wallet"`. Cancelling an order is
NOT wallet-gated — it only reduces risk, so it stays available regardless.
A wallet-lookup failure (e.g. the database is unreachable) fails closed
(blocks), never open.

### Order type

Robinhood's Crypto Trading API supports `type=limit` with
`limit_order_config: {asset_quantity, limit_price, time_in_force}` — `"gtc"`
(good-till-canceled) is the only documented `time_in_force`, no IOC/FOK/
expiry. With `ROBINHOOD_USE_LIMIT_ORDERS=true` (the default), GRID sends a
*marketable* limit: buy at `ask * (1 + slippage_bps/10000)`, sell at
`bid * (1 - slippage_bps/10000)` — priced to cross the spread and fill like
a market order, with `ROBINHOOD_LIMIT_SLIPPAGE_BPS` (25bps default) as
headroom against the price moving between quote and fill. Quantity is still
sized off the raw touch price (ask/bid), not the padded limit price.
Because `"gtc"` never expires on its own, `RobinhoodCryptoTrader.reconcile_stale_orders()`
is the explicit cancel-after handling for a limit that doesn't fill
immediately: it cancels our own LIVE orders resting past
`ROBINHOOD_LIMIT_CANCEL_AFTER_S` (15s default). It now runs **automatically**
at the start of every LIVE (non-simulated) `open_position`/`close_position`
call — if it can't confirm the order book is clean (the order list couldn't
be fetched, or a stale order couldn't be confirmed cancelled), the new order
is blocked with `status="blocked", guard="reconcile_failed"` rather than
stacking on top of a book we can't currently verify. That per-order call
only reconciles at the moment of the *next* order, though — a resting order
could sit stale for a long time if no new order comes in. **Wiring
`reconcile_stale_orders()` into a periodic job (a Hermes step or otherwise),
independent of new orders, is a go-live prerequisite** — see the checklist
below. Setting `ROBINHOOD_USE_LIMIT_ORDERS=false` falls back to plain market
orders, still behind the stale-quote and spread guards (and
`reconcile_stale_orders()` no-ops for market orders, since there is no
resting limit to reconcile).

### Go-live checklist

Before ever setting `ROBINHOOD_LIVE_TRADING=true`, beyond the account
set-up above:

1. **Migration applied.** `migrations/versions/robinhood_guards_20260924.py`
   has run against the target database — see "Persisted state" above.
   Orders fail closed (raise) otherwise, so this is self-enforcing, but
   confirm it before the first live order rather than discovering it then.
2. **Tracking wallet ACTIVE.** See "Tracking wallet" below — every order on
   all three paths is blocked without one.
3. **`reconcile_stale_orders()` wired into a periodic job**, independent of
   new orders — the automatic pre-order call alone is not sufficient if the
   bot goes quiet after a partial/unconfirmed fill (see "Order type" above).
   Not implemented by this change; needs a scheduler entry (Hermes or
   otherwise) before go-live.
4. **Alert routing confirmed** (`ALERT_EMAIL_ENABLED` and the destination in
   `alerts/email.py`'s settings) — every LIVE order and every guard trip
   alerts; verify those emails actually arrive before relying on them.

### Simulate / forward paper log

`RobinhoodCryptoTrader.simulate_order(ticker, direction, size_usd)` and
`.simulate_close(ticker)` run every guard (wallet, drawdown, daily loss,
order rate, stale quote, spread) against real current data and return the
same full decision record `open_position`/`close_position` would in
DRY_RUN — but never POST/cancel an order, and never touch the persisted
order-rate counter, the idempotency log, or wallet P&L. This is the API a
forward paper log should call to see "what would happen" without
consuming an order-rate slot or an idempotency key.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/v1/trading/robinhood/status` | mode, caps, account |
| GET | `/api/v1/trading/robinhood/balance` | buying power, holdings value, equity, HWM |
| GET | `/api/v1/trading/robinhood/positions` | holdings valued at mid |
| GET | `/api/v1/trading/robinhood/orders?limit=50` | recent orders |
| POST | `/api/v1/trading/robinhood/trade` | `{ticker, direction, size_usd, idempotency_key?}` |
| POST | `/api/v1/trading/robinhood/close` | `{ticker, idempotency_key?}` sell the whole holding |
| POST | `/api/v1/trading/robinhood/orders/{id}/cancel` | cancel an open order |

All endpoints require the normal GRID bearer token. `idempotency_key` is
optional; repeating it returns `status="duplicate"` instead of resubmitting.
A tripped guard (drawdown, daily loss, order rate, wallet, stale quote, wide
spread) returns HTTP 200 with `status="blocked"` and a `guard` field, not a
400 — it's a deliberate risk decision, not a malformed request.

`GET /api/v1/system/health` carries a `checks.robinhood` block (mode,
configured, live_trading, and every cap above:
max_position_usd/max_drawdown_pct/max_daily_loss_pct/max_orders_per_day/
max_quote_age_s/max_spread_bps/use_limit_orders/limit_slippage_bps). It
reads local config only — health never calls Robinhood or the database for
this block — and reports `mode: ERROR` plus a degraded reason when the key
in `.env` cannot be loaded.

## Wiring into the paper → live chain

The same proof chain as Hyperliquid applies (`.claude/skills/trading-pipeline`):
paper P&L positive, oracle scoring positive, then a small live allocation.
Every step below stays dry-run until `ROBINHOOD_LIVE_TRADING=true`.

### 1. Tracking wallet

**Required, not just informative, as of the 2026-09-24 guards**: no ACTIVE
`trading_wallets` row for `exchange='robinhood'` means every order — on all
three paths — is blocked with `status="blocked", guard="wallet"`. Create one
before anything else:

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
