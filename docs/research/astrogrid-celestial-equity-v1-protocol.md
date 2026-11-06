# Astrogrid mystical engines versus equity controls — frozen v1

User scope: Astrogrid is the multi-mysticism oracle. The BTC/ETH/SOL taker-flow
evaluation does not test celestial/equity relationships. Freeze this protocol
before reading the equity return sample. No tuning to obtain a positive result.

## Data and coverage

- FRED SP500, NASDAQCOM and DJIA daily equity-index levels, 2017-01-01 through
  2026-10-31, from the official public CSV download. These are equity indexes,
  not ETF fills or every individual stock. They avoid mixing the existing
  unverified per-ticker resolved price histories. Record URLs, source hashes,
  retrieval clocks, actual common sessions and missingness. No price imputation.
- Controls: VIXCLS, DGS10, T10Y2Y, BAMLH0A0HYM2. Use values lagged two actual
  common trading sessions, at most five sessions stale. Historical vintages are
  current-vintage retrospective observations, not a historical live replay.
- Reuse `astrogrid_shared/ephemeris.js` and the actual twelve engine functions
  in `astrogrid_shared/prophecy.js`, in chorus mode and without market inputs.
  Deterministic positions, lunar/Vedic states, raw feature traces and engine
  scores only. No LLM reconstruction of historical answers. Record code hashes.
- Inputs cover the implemented twelve stylized engines and computed bodies;
  no claim to test every possible mystical idea, true fixed-star astrometry,
  random tarot draws, or unavailable measured solar weather. Record missing
  inputs instead of claiming their absence proves zero effect.

## Time separation and predictions

- Initial training 2017–2020. Reference evaluation 2021–2023. Final temporal
  test 2024-01-01 through 2026-10-31. Refit on January 1 using only rows whose
  complete forward label ended before that date; no future training labels.
  No hyperparameter or asset selection using reference/test outcomes.
- Forecast one, five and twenty actual common market sessions. Inputs at
  session d; enter at close d+1, exit close d+1+h. Sample test decisions every
  h sessions per asset, fixed from first eligible date, to avoid overlapping
  holding intervals. Require >=100 test events and >=30 distinct dates per
  asset/horizon for inferential eligibility; otherwise report insufficient.
- Fixed ridge regression, lambda=100; train-only standardization, intercept
  unpenalized. Predict returns, not calibrated probabilities. Standardize and
  clip test features at eight training standard deviations. Never change the
  live oracle/weights automatically from these exploratory results.

## Model comparisons

- Market-only control model: target 1/5/20-session past momentum, 20/60-session
  realized volatility, 200-session trend, trailing market-index momentum,
  lagged VIX/rates/curve/credit levels and five-session changes, calendar
  weekday and annual sine/cosine terms; past-price trend and volatility states
  define regime proxies. No hindsight regime-history backfill.
- Add one implemented engine score at a time (12), all twelve together, or all
  deterministic sky features (angles encoded sine/cosine, categorical states
  one-hot, retrogrades, aspects, lunar phase, nakshatra and tithi). Sky features
  may interact with past trend and high-volatility states. Same training,
  sessions, model capacity convention and execution prices in every ablation.
- Diagnostic controls: always long, cash, price momentum, analytic random
  direction, mystical-only ridge, and the actual unmixed Seer direction.
  Do not substitute the best discovered variant for the existing oracle.
- Report out-of-sample squared forecast error, directional accuracy, gross/net
  fixed-notional return and paired improvements versus market-only; break out
  regimes and all declared assets/horizons. No compound portfolio claims.

## Costs and inference

- Primary illustrative equity cost: 1bp commission + 1.5bp spread/slippage per
  side, debit both entry and exit notionals. Show 0/5/10/20bp roundtrip scenarios.
  Index-level paper exposures are not executed ETF/stock trades. Shorts exclude
  borrowing/funding/availability costs and must be labelled hypothetical.
- Joint-date circular moving blocks of 20 trading sessions; 1,999 replicates,
  seed 20261106. Keep assets contemporaneous. Paired means and 95% intervals;
  centered two-sided bootstrap p with +1 correction. Benjamini–Hochberg across
  the full 14 augmentations x3 assets x3 horizons (126 tests), including
  insufficient/constant cases as p=1. Do not prune failed trials from the family.
- Historical candidate support requires eligible sample, paired net advantage
  over market-only with 95% lower bound >0, q<=.05, and positive mean net with
  95% lower bound >0. Failure means not demonstrated by this specified test,
  never a universal assertion that celestial associations cannot exist.
- Prospective forecasts, if subsequently generated, must freeze asset, horizon,
  input clock, direction/target and invalidation before outcomes and remain a
  distinct cohort. Existing synthetic seed/backtests cannot count as live proof.

No trading, provider privileges, paid inference, historical ledger rewrites,
global eligibility changes or automatic model promotion.

## Availability amendment before any model fitting or return comparison

The official FRED multi-series ZIP includes only about three years of the
ICE high-yield series, although the declared sample is longer. Recover that
same control from the existing `hy_oas_spread` resolved series through bounded
`PITStore.get_pit`, with database-enforced read-only connections. All 779 common
dates match the official download exactly (maximum difference zero). Retain
reader/provenance/source-content hashes and latest-vintage caveat. This changes
no target, test boundary, model, hypothesis or inference rule.

Shared JS ephemeris omits Sun; supply Sun using the existing production Python
Earth-longitude +180 degree formula. All date-only positions use 00:00 UTC as
in the production snapshot, whose label is 12:00 UTC. Record this approximation
and source hashes. Fixed-star astrometry and measured historical solar weather
remain outside this implemented-input test. No model outcomes have been read
when making these coverage repairs.
