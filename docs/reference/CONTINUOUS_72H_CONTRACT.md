# Continuous venue-specific 72h evaluation (local candidate)

Base snapshot: `d25c85323535bab82e760c6512c03e053e155d6e` from the existing
Actions checkout. This separate module does not modify `evaluation/prices.py`,
NYSE policy, TimesFM defaults, VS1 v7 preregistration/power, GEX 60-session
forward protocol, scheduler, ingestion, database, or trading paths.

`evaluation.continuous_forecasts` evaluates caller-supplied immutable records
in memory. It imports no database/provider clients. A real adapter remains
unimplemented until accepted venue-specific timestamp/basis evidence exists;
any future adapter must use the existing PIT/availability read controls.
Date-only YF rows cannot be converted into intraday bars by this module.

## Required evidence

- Explicit venue, instrument, spot/perpetual market, `24/7` calendar, positive
  cadence dividing exactly 259200 seconds (at most 24h), and reviewed evidence
  reference. A cadence of 1h requires 72 steps; 30min requires 144. A service
  default of 128 steps proves neither. All times are timezone aware and are
  compared as UTC instants across weekends, holidays and DST.
- Explicit raw trade close or perpetual mark close basis, uniform across all
  bars. Adjusted or mixed basis, duplicate/unordered/missing bars, invalid
  prices and missing provenance refuse. Mark-price returns remain mark-price
  scenarios, not fills. Source references identify immutable row vintages.
- Frozen forecast origin, issue time, latest input availability, model/version
  and frozen input reference. Latest input availability <= issue <= origin;
  all history availability <= input cutoff. Last input closes before origin,
  no more than one cadence earlier; at least two regular historical bars are
  required. This permits publication/computation delay without future input.
- Exact outcome grid from origin + cadence through origin + 72h, plus a
  separate exact-origin entry bar. Entry is outcome evidence, not an inference
  input or presumed tradable quote. Outcome and entry availability must be
  within the evaluation cutoff. Immature horizons return unavailable/null
  without inspecting exit evidence. Mature missing evidence raises
  `ContractError`, never fabricates, fills, rolls or selects a successor bar.
- Point and explicitly labelled q10/q50/q90 arrays matching every target.
  Quantiles must be positive finite and noncrossing. They must actually be
  those quantiles; generic TimesFM bounds/std cannot be silently relabelled.
- Explicit fees/slippage per side and signed cumulative funding debit for a
  unit-notional long over the complete [origin, origin+72h] interval. Costs
  require evidence, availability and an assumption flag; no defaults. Spot
  funding must be zero. A negative funding value is a credit. The cost
  reference must establish complete funding coverage; this module does not
  independently reconstruct payment events. Realized funding is outcome
  evidence available after horizon completion, never an inference feature.

## Metrics and readiness

Price MAE/RMSE and terminal return error compare the model with persistence
(last available input price) and linear drift (slope across the full historical
window, projected by elapsed cadence). No baseline uses future data. Negative
linear extrapolations are retained as benchmark predictions, never fills.
Pinball loss at .1/.5/.9, 80% interval coverage and width diagnose uncertainty.
One path's serially correlated bars do not establish calibration, power,
independent sample size, significance or alpha. Use a separately frozen
multi-origin protocol with overlap-aware uncertainty before acceptance; this
patch makes no such claim and does not fit/recalibrate on outcomes.

The long scenario is `100*(exit/entry-1) -
(2*(fee_bps_per_side+slippage_bps_per_side)+funding_bps_long)/100`.
It is a fixed unit-notional arithmetic scenario, not compounded funding,
leverage, margin, liquidation, executable PnL or a strategy backtest. Costs
label assumptions explicitly. All derived metrics must remain finite.

`preflight` validates frozen input/grid readiness only and always returns
`activation_authorized: false`. It calls no service or scheduler. It does not
retry the expired September 30 activation or grant any operational permission.

## Reproduction and validation

From repo root with the existing local Python environment:

```
PYTHONPATH=. python scripts/evaluate_continuous_fixture.py
python -m pytest -q tests/test_continuous_forecasts.py tests/test_signal_outcomes.py tests/test_evaluation_prices.py tests/test_evaluate_signals_cli.py
ruff check evaluation/continuous_forecasts.py scripts/evaluate_continuous_fixture.py tests/test_continuous_forecasts.py
```

`docs/examples/continuous_72h_synthetic.json` is a deterministic, explicitly
synthetic Saturday/DST fixture with five-second bar publication delay. Its
linear-plus-one predictor is deliberately constructed, not TimesFM inference.
Its MAE is 1 quote unit; synthetic interval coverage is 1; these are software
checks, not economic evidence. The script uses strict finite JSON serialization.

Focused run: **119 passed**, including 45 new calendar/horizon/leakage/missing
bar/basis/cost/extreme-value tests. Ruff check passes after formatting.
A broader yfinance-basis run: **119 passed, 11 failed**, all 11 failures from
missing local `yfinance`. Alembic single-head collection was **unrun** due to
missing `alembic`. Full suite, live TimesFM and real-bar integration unrun.
No dependencies were installed and no paid provider calls were made.

## Bounded real-data availability probe (2026-09-30)

Existing strict-known-host SSH was used read-only. Local Mac GRID base is
`f09cbb86e88019f8fe73f57a4e892e148a1fe711`; server development tree is
`5facbdf0d473f576899305d8358a08f28202c91a` and dirty. Both predate the selected
evaluation snapshot; neither was edited. The Actions tree also has unrelated
deployment-hook dirt, excluded from the archived committed snapshot.

A `psql -X -w` session reached database/user `grid|grid`, began READ ONLY,
set a 3000ms statement timeout and rolled back. Bounded information-schema
and source-catalog queries confirmed `public.raw_series` and
`public.source_catalog`; raw observation type is DATE, pull timestamp is
TIMESTAMPTZ; no typed venue/basis/origin columns appear in that table. The
bounded names matching hyperliquid/crypto/yfinance returned only `yfinance`.
The queried table names did not include a `crypto_bars` table. This proves
only this database/schema/catalog probe, not that crypto data is absent from
all databases, JSON payloads, files or hosts. No raw price, forward label,
discovery or holdout outcomes were queried. Existing Hyperliquid puller source
emits context signals, which does not establish the required bar history.

The earlier SIGNAL_EVALUATION_DRYRUN_MANIFEST still governs the equity dry
run. Database reachability and actual raw column types are now observed here;
its query integration, historical raw-close basis cutover, source/series
availability, row-level origin/publication provenance and quality remain open.
No production evaluator was run. This patch neither asserts nor injects a
cutover. Real 72h validation additionally needs frozen venue-specific bars,
complete funding/cost provenance and an accepted multi-origin cohort protocol.

## Pending decisions

Publication requires user approval: no push, PR or merge was performed.
Deployment, scheduler activation, trades, paid APIs and production writes are
outside this task. Fleet reporting and TODO updates were prepared locally
in `LOCAL_REPORT.md` rather than posted to the production reporting hub/vault,
consistent with the explicit no-production-write/local-patch instruction.

Independent read-only review completed after latency/finite-metric repairs; no remaining blocking defect found. Forecast origins must align with actual bar boundaries or accepted aggregation; arbitrary issue times cannot shift hourly bars.
