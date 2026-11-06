# Astrogrid spot-flow edge evaluation v1

Frozen before downloading or inspecting evaluation prices, 2026-11-06 UTC.
Rule/model: `astrogrid-spot-flow-v1` / `binance_spot_taker_30m_v1`, introduced
in PR993. This evaluation does not tune or change that rule.

## Sample and origin

- BTCUSDT, ETHUSDT, SOLUSDT Binance global spot monthly 5m archives, November
  2025 through October 2026 inclusive. Verify provider SHA256 files and strict
  bar validity, uniqueness and UTC cadence. Missing files/bars are reported;
  never impute prices or substitute another venue. Normalize documented
  microsecond archive timestamps to milliseconds explicitly.
- Historical replay of current archive vintages, not prospective forecasts and
  not evidence of historical live availability/execution. Never insert replay
  data into the forecast ledger or live receipts.
- Reference period: November 2025 through July 2026. Untouched temporal test
  period: August through October 2026 inclusive (92 UTC dates). No fitting,
  threshold selection, asset selection or retries to improve a result.
- Require >=500 eligible nonoverlapping test events, >=60 distinct test UTC
  dates and >=100 events per asset for a useful historical comparison. These
  are reporting floors, not guarantees of statistical power or market diversity.

## Event and outcome contract

- Original fixed input: completed 5m bar, >=100,000 USDT turnover, >=100
  trades, absolute signed taker imbalance >=10%. Sign is +1 for buy, -1 sell.
- Assume issuance 15 seconds after bar end. Original contract due is 30m after
  issuance, so the first completed 5m outcome ends 35m after the signal bar.
- Report original close-to-outcome forecast returns separately. Primary cost
  comparison enters at the next completed 5m close (+5m), exits at the same
  declared outcome (+35m): a conservative timing scenario, not actual fills.
  This avoids credit for an unavailable signal-bar closing fill. Require every
  intervening bar and keep entry/outcome within the same sample partition.
- Greedily retain earliest qualifying event per asset, then skip events whose
  signal end precedes the retained outcome end. Selection uses only timestamps
  and already-closed input bars, never forward returns. This removes overlapping
  exposure per asset; bootstrap resamples all assets jointly by UTC day to
  retain contemporaneous dependence.

## Matched baselines and costs

- Same events, prices, delays, horizon and cost assumptions for flow direction,
  always long, always short, last closed 5m price momentum (close versus open),
  reversed flow, and a 50/50 random direction's analytic expected return. Cash
  has zero return/cost. Flat momentum bars abstain.
- Main scenario: fee 10 basis points per side, slippage/spread allowance 2bp
  per side. Fees use current regular-user Binance spot rates as a scenario;
  do not claim historical or account-specific fees. Debit costs on both entry
  and exit notionals: `(fee+slippage)/10000 * (1+exit/entry)` per active event.
- Show gross, fees-only, main net and roundtrip cost sensitivity 0/8/20/24/40bp.
  Report mean bp per fixed-notional event; never sum overlapping forecasts as
  a compound portfolio or annualize their Sharpe.
- Short direction on spot is a hypothetical directional paper position; actual
  borrowing, funding and availability are not modeled. Also report an executable
  direction restriction: long on buy, cash on sell, with costs only when long.
  Neither scenario is an executed account result.

## Inference and interpretation

- Deterministic circular seven-UTC-day moving-block bootstrap, 1,999 replicates,
  seed 20261106. All assets sampled together; ratio of sums to event counts.
  Show 95% bootstrap intervals on mean net and paired advantages over baselines.
- One predeclared primary pooled test. To label historical support, sample
  floors must pass and simultaneous one-sided Bonferroni bootstrap lower bounds
  (alpha .05 divided by comparisons) must exceed zero for mean main net versus
  cash and paired differences versus always long, always short, momentum and
  expected random direction. Reverse flow is diagnostic, never selected as a
  replacement. Per-asset/month/reference-period analyses are descriptive.
- If mean net's 95% upper bound <=0: no positive net return supported under
  these costs. If floors fail: insufficient sample. Otherwise, if simultaneous
  gates fail: no demonstrated edge against the baselines. Positive historical
  support still requires prospective confirmation and executable fill/borrow
  evidence; never promote weights or change the live rule automatically.
- Prospective results remain a separate cohort with actual issuance/source
  availability checks; do not pool historical replay and live forecasts.

Sources: https://github.com/binance/binance-public-data and
https://www.binance.com/en/fee/trading (regular spot taker fee: 0.100% per side).
