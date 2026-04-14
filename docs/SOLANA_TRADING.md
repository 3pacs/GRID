# Solana Trading (AutoHedge-derived)

The `trading/solana/` package is GRID's foundation for autonomous Solana trading.
It is inspired by [The Swarm Corporation's AutoHedge](https://github.com/The-Swarm-Corporation/AutoHedge)
(MIT), with the reusable pieces — the Jupiter Ultra / Price V3 API client and
the Director → Quant → Risk → Execution agent pipeline — ported into GRID and
rewired onto existing GRID infrastructure.

## What's included

| Module | Purpose |
| --- | --- |
| `trading/solana/jupiter_client.py` | Thin HTTP client for Jupiter Price V3 and Jupiter Ultra swap APIs. Returns typed `SwapOrder` objects and wraps all errors in `JupiterError`. |
| `trading/solana/wallet.py` | `SolanaWallet` helper that lazy-imports `solders`. Modules above it can be imported without the Rust wheel present. |
| `trading/solana/solana_rpc.py` | Minimal JSON-RPC client — parses SPL token mint accounts (mint/freeze authority renouncement) and fetches `getTokenLargestAccounts` for holder concentration. No `solders` dependency. |
| `trading/solana/safety.py` | `SolanaSafetyChecker` — rug / honeypot / concentration / price-impact gate. Produces a `TokenSafetyReport` with per-check severity (`block`/`warn`/`info`). |
| `trading/solana/limits.py` | `DailyLimits` — per-day USD, trade-count, and per-mint caps enforced against the `paper_trades` table. Fails closed on DB error. |
| `trading/solana/pipeline.py` | Stateless 4-stage pipeline (Director, Quant, Risk, Execution) using GRID's `llm.router` instead of Swarms. Produces a `PipelineDecision`. |
| `trading/solana/executor.py` | `PaperSolanaExecutor` — runs every trade through **safety → limits** before routing to paper or live. On open, hands the new trade to the exit manager so exits are managed immediately. |
| `trading/solana/exit_policy.py` | Immutable `ExitPolicy` / `ExitRung` dataclasses plus four seed variants (`conservative`, `balanced`, `aggressive`, `scalper`) — the learner's starting arms. |
| `trading/solana/exit_decision.py` | Pure function `decide_exit(state, policy) → ExitAction`. Entire exit logic with zero I/O; trivially unit-testable. |
| `trading/solana/exit_state.py` | DB-backed state — `solana_exit_state` (per-position peak / remaining / rungs_hit / trailing_armed), `solana_exit_events` (immutable audit log), `solana_policy_variants` (learner posterior). Schema auto-created. |
| `trading/solana/exit_learner.py` | `ExitLearner` — contextual Thompson-sampling bandit over policy variants, specialised per `(variant_id, source_type)` pair. Online Welford updates, reward clipping to `[-1, 3]`. |
| `trading/solana/exit_manager.py` | `ExitManager.tick()` — one pass over every open Solana position: fetch price → `decide_exit` → record event → partial or final close → feed learner on final close. |

## Safety rails

Every trade — paper or live — goes through two gates before it touches
`paper_trades` or Jupiter Ultra:

1. **`SolanaSafetyChecker`** runs four checks against the target mint:
   - `mint_authority` — is the mint authority renounced? (dev can't print)
   - `freeze_authority` — is the freeze authority renounced? (dev can't freeze you)
   - `holder_concentration` — top-10 non-burn holders own < N% of supply
   - `price_impact` — simulate the intended sell via Jupiter Ultra and
     compare `outAmount` against spot price; reject if slippage > N%
   Each check has a severity: `block` (fails the gate), `warn` (logged,
   does not block), or `info`. Disabling a requirement in config
   downgrades its severity to `warn` so the report always tells the truth.

2. **`DailyLimits`** enforces per-day caps using SQL aggregates over
   `paper_trades` rows for the current UTC day:
   - `max_daily_usd` — total notional
   - `max_daily_trades` — trade count
   - `max_per_mint_daily_usd` — per-mint cap
   Any failure puts the decision in the `skipped` bucket and records the
   reason on the `ExecutionResult`.

Tuning lives entirely in `config.py` (`SOLANA_*` settings) so you never
have to edit the checker to adjust thresholds.

## Exit manager & self-learning

Exits are where most Solana bots die, so GRID runs exits through a
dedicated tick loop (`ExitManager.tick()`) backed by a contextual
multi-armed bandit (`ExitLearner`).

### Policy variants

Four seed variants cover the memecoin archetype space (see
`trading/solana/exit_policy.py`):

| Variant | TP ladder | SL | Trail | Max hold |
| --- | --- | --- | --- | --- |
| `conservative` | +30/+60/+120% (34/33/33%) | -15% | -10% after +25% | 30m |
| `balanced`     | +50/+100/+200% (33/33/34%) | -20% | -15% after +50% | 60m |
| `aggressive`   | +80/+200/+500% (25/25/25%) | -30% | -20% after +80% | 120m |
| `scalper`      | +15/+30% (50/50%)          | -8%  | -5% after +10%  | 10m |

### Decision precedence

Every tick, `decide_exit(state, policy)` walks these rules in order:

1. Hard stop-loss
2. Max-hold timer
3. Trailing stop (only if armed)
4. Take-profit rung
5. Arm trailing stop
6. Hold

The function is pure — no I/O, no RNG — so every branch is a unit test.

### Self-learning

The learner is a **Thompson-sampling contextual bandit** over
`(variant_id, source_type)` arms:

- On position open: sample one reward from each arm's Gaussian posterior
  (`Normal(μ, σ / √(n+1))` with a weak prior), pick `argmax`, assign that
  variant to the trade. Unseen arms use an optimistic wide prior to force
  exploration.
- On final close: compute the **blended realised pnl** across every
  exit event for that trade, clip to `[-1, +3]` (memecoin tails are fat),
  and apply one online Welford update to `(n, μ, M²)`.
- Posterior state lives in `solana_policy_variants` — survives restarts
  and is visible via `ExitLearner.stats_snapshot()` for dashboards.

The bandit is *contextual* on `source_type` so the system automatically
specialises once signal-source metadata is threaded through (e.g. one
variant may dominate for `smart_money` while another wins for `kol`).
In the meantime every trade lives under `source_type="unknown"`.

### Audit trail

Every partial close, stop fire, or trailing-arm event is appended to
`solana_exit_events` — immutable by convention, matching GRID's
decision journal rule. The blended exit price passed to
`paper_engine.close_trade()` is computed as
`Σ(event_fraction × event_price) / total_closed_fraction`, so
`paper_trades.pnl_pct` reflects the laddered exit, not just the last
slice.

### Running it

```python
from sqlalchemy import create_engine

from config import settings
from trading.solana import ExitManager, ExitLearner, JupiterClient

engine = create_engine(settings.DB_URL)
jupiter = JupiterClient(api_key=settings.JUPITER_API_KEY)
learner = ExitLearner(engine=engine)
learner.ensure_variants()

manager = ExitManager(engine=engine, jupiter=jupiter, learner=learner)

# Wire into your scheduler — run every 30-60s
summary = manager.tick()
print(summary.to_dict())
```

To see what the learner has learned:

```python
for row in learner.stats_snapshot():
    print(row)
```

## Why not use AutoHedge directly?

AutoHedge pulls in the Swarms framework, OpenAI SDK, and its own CLI. We want to:

* depend only on libraries we already vendor in GRID
* reuse `trading/paper_engine.py`, the decision journal, and the circuit breaker
* honour GRID's PIT correctness rules and 3-tier LLM taxonomy (`LOCAL`, `REASON`, `ORACLE`)

So we ported the 2 pieces of AutoHedge that are actually doing meaningful work
(Jupiter client, agent prompts), rebuilt the orchestration on our own LLM
router, and kept everything else — risk, execution, audit — in GRID idioms.

## Environment variables

Add to `.env` (already declared in `config.py`):

```
JUPITER_API_KEY=...            # optional; unlocks rate limits
SOLANA_PRIVATE_KEY=...         # base58 wallet key; required for live mode
SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
SOLANA_LIVE_TRADING=false      # must be true to place real swaps
SOLANA_MAX_POSITION_USD=50.0
SOLANA_MAX_DRAWDOWN_PCT=0.20
```

## Optional dependency

Signing live transactions needs the `solders` Rust wheel:

```
pip install solders
```

The package is deliberately **not** in `requirements.txt` so CI and paper-mode
deployments don't pay the build cost. Paper mode works without `solders`.

## Usage

```python
from sqlalchemy import create_engine

from config import settings
from trading.solana import SolanaPipeline, JupiterClient
from trading.solana.executor import PaperSolanaExecutor

engine = create_engine(settings.DB_URL)
jupiter = JupiterClient(api_key=settings.JUPITER_API_KEY)

pipeline = SolanaPipeline(jupiter=jupiter)
decision = pipeline.run("Analyse SOL momentum for today")

executor = PaperSolanaExecutor(engine=engine, jupiter=jupiter, live=False)
result = executor.execute(decision)
print(result)
```

## Going live (checklist)

1. Run the pipeline on paper for at least one week.
2. Fund a dedicated Solana wallet (NOT your personal keypair).
3. `pip install solders`.
4. Set `SOLANA_PRIVATE_KEY` and `SOLANA_LIVE_TRADING=true` in `.env`.
5. Wire a `SolanaWallet` into `PaperSolanaExecutor(live=True, wallet=...)`.
6. **Translate `size_fraction` into atoms upstream** — the live path refuses
   fractional sizes because Jupiter Ultra expects raw integer amounts. This is
   a deliberate guard against accidentally sending a portfolio-fraction value
   as lamports.

## Tests

```
python -m pytest tests/test_solana_jupiter.py tests/test_solana_pipeline.py tests/test_solana_executor.py -v
```

All HTTP and LLM traffic is mocked — no network or real wallet required.

## Attribution

AutoHedge is MIT-licensed. The Jupiter Ultra / Price V3 client and the four
agent prompts in this package are derivatives of upstream code; see the
docstrings in each module for pointers to the specific AutoHedge files.
