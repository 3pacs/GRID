# Astrogrid mystical-input equity test

Astrogrid is the multi-mysticism reader. The earlier taker-flow report tests a
separate crypto component; it cannot answer whether celestial inputs predict
equities. This study evaluates the implemented twelve engine scores and 245
computed sky features, with market/regime/calendar controls. It does not replay
historical LLM answers or test every possible mystical idea.

## Frozen experiment

[Protocol](astrogrid-celestial-equity-v1-protocol.md) was committed as
`3fe178a4` before price downloads. Coverage amendments and the implementation
were committed before fitting. No threshold, asset, hyperparameter or validation
gate was selected to improve a result. Before publication, invariant tests
corrected training scaling and restored the protocol's missing zodiac one-hot
columns. Final engine/ephemeris, evaluator, protocol and input hashes are retained
in the published JSON; only the corrected implementation feeds these findings.

Official FRED S&P 500, Nasdaq Composite and Dow index levels, 2017–October 2026,
provide 2,471 common market sessions. External controls are VIX, ten-year Treasury
yield, yield curve and high-yield credit spread; levels and changes are lagged
two common market sessions. FRED limits the public credit download's depth;
bounded database-enforced read-only `PITStore.get_pit` recovered that same series.
All 779 overlapping dates matched the official download exactly. No price
imputation, mixed ticker history, future regime backfill or ledger writes.

The actual shipped shared mystical engines and browser copies match byte for
byte. Compute at the production date-only clock and add Sun using the production
Earth-opposition formula. Inputs include Sun/Moon/planet/node angles, all twelve
zodiac states per body, retrogrades, aspects, lunar phase, nakshatra and tithi.
The engine scores differ by lens, but share sky inputs and are not twelve
independent observations. Approximate ephemeris, unavailable fixed-star astrometry
and measured solar weather remain explicit limitations.

Initial training is 2017–2020; 2021–2023 is a reference period. The final test is
2024–October 2026, with annual refits using only labels completed before each
year. Fixed ridge100, train-only scaling, no model/parameter search. Market
controls include momentum, realized volatility, trend, broad market moves,
macro controls, weekdays, month/annual seasonality and calendar effects. Add
each engine separately, all twelve together, or all sky features; include
interactions with past trend and volatility states.

Predictions enter at the next common session close and exit 1/5/20 sessions
later. Nonoverlapping exposures are selected within each case. The test has
709 daily, 141 five-session and 35 twenty-session events per index; the final
horizon fails the predeclared 100-event sample floor. Costs are illustrative:
1bp commission plus 1.5bp spread/slippage per leg, charged on both notionals.
These are index-level paper exposures, not executable ETF/stock fills. Shorts
exclude borrow/funding/availability. Means are fixed-notional bp per event,
not a compounded or annualized portfolio.

## Main result

There are 126 declared comparisons (14 augmentations x3 indices x3 horizons).
84 are sample-eligible; 42 are insufficient. No comparison passes the corrected
net-return improvement gate, or the combined positive-net-return gate. This
finite test does not establish that celestial relationships cannot exist.

All twelve engine features added to the market model:

| Index | Horizon | Events | Net bp | Market-only net bp | Always-long net bp | Added net bp / 95% interval |
|---|---:|---:|---:|---:|---:|---|
| S&P 500 | 1 session | 709 | -0.68 | -1.40 | +2.18 | +0.72 [-2.01, +3.61] |
| S&P 500 | 5 sessions | 141 | +11.26 | +22.31 | +31.83 | -11.04 [-43.18, +23.54] |
| Nasdaq | 1 session | 709 | -2.69 | -1.05 | +3.69 | -1.64 [-7.39, +3.65] |
| Nasdaq | 5 sessions | 141 | +34.54 | +5.53 | +40.28 | +29.01 [-15.40, +79.58] |
| Dow | 1 session | 709 | -4.63 | -3.94 | -0.05 | -0.69 [-5.26, +3.94] |
| Dow | 5 sessions | 141 | +10.53 | +23.68 | +20.57 | -13.14 [-33.09, +5.82] |

Positive point estimates are present. For example the Nasdaq five-session
hybrid outperforms this particular market-only control model in the sample,
but its paired interval spans zero and always-long does better. Positive
paper returns alone can reflect market drift. Neither this example nor
individual/regime slices should be selected retrospectively as a validated
replacement oracle.

Inference uses 1,999 deterministic 20-trading-session circular moving-block
bootstrap replicates; same-date resampling is aligned across assets. Paired
two-sided centered-bootstrap p-values receive Benjamini–Hochberg correction
over all 126 trials, including insufficient trials as p=1. Regime slices and
reference results are descriptive. Squared forecast errors are also retained;
the predeclared validation gate concerns net directional returns.

## Verification and product behavior

- Independent blind audit re-read original provider CSVs and reconstructed
  seven of 126 cases (>5%) using augmented-design least squares rather than
  the evaluator's normal-equation solve. All event counts, nonoverlap rules,
  net means and paired means matched. Source series and credit overlap matched.
- Tests cover past-label purging, future-price invariance of earlier forecasts,
  unpenalized intercept/scaling with outliers, both-leg costs, deterministic
  blocks, full multiple-testing family and exact report/protocol hashes.
- JavaScript tests run the actual twelve engines, require all zodiac states,
  verify deterministic captures and reject missing Sun input. Browser/shared
  engine files have matching hashes. No historical LLM answers are fabricated.
- The Oracle remains the first product surface. Its chosen mystical reading
  and lens permissions are preserved; a measured-flow lookup no longer
  overwrites the answer. Market evidence is separate context. Research and
  track-record details persist across refreshes. No statistical finding changes
  the reader's live weights, original ledgers or model eligibility automatically.

[Full results](../../astrogrid_web/research/celestial-equity-v1.json) retain
all 126 comparisons, controls, baseline/cost sensitivities, training-label
audit, regime slices, input limits and source/code hashes. To reproduce with
the recorded captures:

```bash
python scripts/astrogrid_celestial_report.py --panel /capture/panel.json \
  --features /capture/sky-features.json \
  --output astrogrid_web/research/celestial-equity-v1.json
```

Capture preparation uses the recorded FRED ZIP URL, bounded credit PIT export,
the production Sun formula and `scripts/astrogrid_celestial_features.mjs`.
Current historical vintages do not prove historical availability or fills.
The next evidence needed is properly frozen prospective full-reader forecasts;
the original seed corpus and naive lag-optimized correlation cache cannot
substitute for that evidence. Broader assets, nonlinear models, rare events,
fixed stars, solar weather and randomly drawn divinations remain separate
research questions. No blanket rejection of mysticism is inferred.
