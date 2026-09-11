# 06 — Robinhood: activate the account and wire it into the paper → live chain

Branch: `claude/handoff-06-robinhood-wiring`. Lane: code + ops-exec. Needs the operator
to create the API credential (step 1) — everything else can be prepared first.

## Context

`trading/robinhood.py` (merged in #412) speaks Robinhood's official key-signed
Crypto Trading API: account, holdings, best bid/ask, trading pairs, orders;
USD-sized market orders; per-order cap and drawdown halt; **dry-run default**
(`ROBINHOOD_LIVE_TRADING=false`). Routes under `/api/v1/trading/robinhood/*`.
Runbook: `docs/ROBINHOOD_SETUP.md`. Crypto spot only — Robinhood has no
equities API.

## Steps

1. Operator: on grid-svr `python3 -m trading.robinhood keygen`, create the
   credential in the Robinhood app (Account → API) with the public key, put
   `ROBINHOOD_API_KEY` and `ROBINHOOD_PRIVATE_KEY_B64` in the server `.env`,
   restart `grid-api`. You verify with `python3 -m trading.robinhood status`
   via ops-exec (it masks the account number; never print the env).
2. Wallet: create the tracking wallet through the existing API
   (`POST /api/v1/trading/wallets` with `exchange: "robinhood"`, small initial
   capital, 20 % max drawdown) so `trading/wallet_manager.py` tracks P&L and the
   auto-kill applies. Confirm it appears in `/wallets/dashboard`.
3. Rotation trader: `scripts/live_rotation_trader.py` maps regime → BTC/ETH/SOL
   on Hyperliquid perps. Add a `--venue robinhood` option that uses
   `RobinhoodCryptoTrader` for **spot** allocation (long only; risk-off = sell
   to cash), reusing the same target-weight logic. Keep Hyperliquid the default.
   Hermes step 7g (`scripts/rotation_paper_trader.py`) stays paper.
4. Signal executor: `trading/signal_executor.py` opens paper trades; add an
   optional venue tag so a `robinhood` wallet can receive BUY signals for the
   crypto tickers only (`BTC`, `ETH`, `SOL`, `DOGE`, whatever `trading_pairs`
   reports tradable). Everything stays dry-run until the operator flips
   `ROBINHOOD_LIVE_TRADING`.
5. Health: extend `api/routers/system.py::health` with a `robinhood` block
   (mode, configured) — do not add a parallel check endpoint.
6. Tests: mocked `RobinhoodCryptoTrader` in the rotation trader and executor
   paths; wallet creation payload; health block.
7. Proof chain before live money (same as Hyperliquid, see
   `.claude/skills/trading-pipeline`): paper P&L positive, oracle scoring
   positive, then `ROBINHOOD_LIVE_TRADING=true` with `ROBINHOOD_MAX_POSITION_USD`
   at $25–$100. Record the decision in the agent report; do not flip the flag
   yourself.

## Done when

`/robinhood/status` shows the account in DRY_RUN, a `robinhood` wallet exists,
the rotation trader can print a Robinhood spot allocation in dry-run, and the
tests pass.
