# Astrogrid spot-flow rule: no net predictive edge demonstrated

Frozen protocol: [v1](astrogrid-spot-flow-edge-v1-protocol.md), committed as
`7ebb04f0` before evaluation-price downloads. Results are a current-vintage
historical replay, not historical live forecasts or executed trades. The live
rule and original ledger remain unchanged.

## Evidence and sample

Binance global spot BTCUSDT/ETHUSDT/SOLUSDT, 5m monthly archives, November 2025
through October 2026. All 36 provider SHA256 checksums verified; 315,360 bars,
zero missing bars, zero duplicate timestamps. Reference period: November–July,
32,151 events. Temporal test: August–October 2026, 92 UTC dates, 10,854 events
(BTC 3,637; ETH 3,621; SOL 3,596). Of 60,752 qualifying test bars, 49,896 were
excluded for overlapping exposure and two purged at the partition boundary.
No parameters were fitted or selected using either period.

Original live gates are unchanged: >=100k USDT turnover, >=100 trades,
absolute taker imbalance >=10%. Replay assumes issuance 15 seconds after
signal end, so the first completed outcome after the 30-minute due time is
+35m. Primary economic comparison enters at the next completed close (+5m),
with the same +35m exit. This is a timing/cost scenario, not fill evidence.

## Matched economic comparison

Mean basis points per fixed-notional event, same retained events, entry and
exit for every baseline. Costs: 10bp fee and 2bp slippage per side, charged on
both entry and exit notionals. No compounding or annualization.

| Rule | Mean net bp | 95% joint-day block-bootstrap interval |
|---|---:|---:|
| Taker-flow direction | -23.94 | [-24.75, -23.08] |
| Cash | 0.00 | [0.00, 0.00] |
| Always long | -23.42 | [-24.57, -22.02] |
| Always short (paper) | -24.58 | [-25.98, -23.43] |
| Prior 5m price momentum | -23.47 | [-24.22, -22.65] |
| Random direction, expected | -24.00 | [-24.00, -24.00] |
| Reverse flow, diagnostic | -24.06 | [-24.93, -23.25] |
| Buy on bullish flow; otherwise cash | -11.72 | [-12.56, -10.82] |

Flow gross mean is +0.05975bp; fees-only mean is -19.94084bp. The paired
advantage over random direction is +0.05975bp, 95% interval [-0.74898,
+0.92549]bp. Advantages over always-long and momentum are negative; their
paired intervals also span zero. The original signal-close target-hit rate
is 30.7352%, direction accuracy 48.2495%; these are separate from delayed
entry and net profitability. A target hit is not an executable profitable trade.

Inference uses 1,999 deterministic seven-day circular moving-block bootstrap
replicates, sampling all assets together. The predeclared historical-support
test requires simultaneous Bonferroni lower bounds >0 against cash, always
long, always short, momentum and analytic random direction; none clears its
gate. The net-return interval is entirely negative. The conclusion is
`NO_POSITIVE_NET_RETURN` under the main cost assumptions.

At roundtrip allowances 0/8/20/24/40bp, mean net is respectively
+0.06/-7.94/-19.94/-23.94/-39.94bp. Even zero-cost gross results do not
establish direction skill relative to random, and lower-cost scenarios
remain negative. Reference, monthly and per-asset results are retained in
the full JSON; they were not used to choose a replacement rule.

## Limits and reproducibility

Current regular-user [spot fees](https://www.binance.com/en/fee/trading) are
a scenario, not proof of historical or account-specific fees. Spot shorts
are hypothetical and exclude borrow, funding and availability constraints.
Costs on shorts are optimistic; the separately reported long/cash variant
also loses money in this test. Archive close prices do not prove available
fills or historical source receipt times. The temporal test is retrospective,
even though its protocol was fixed before this analysis.

The independently computed decimal audit re-read 12 provider archives
(including July boundary inputs), matched all 10,854 selected timestamps,
and reproduced flow/always-long/always-short/momentum net means within
1e-9bp. Method tests check delayed entry, due-time rounding, future-price
invariance of selection, overlap removal, missing/boundary exclusion,
timestamp units, both-leg cost accounting, and joint-asset block sampling.

Reproduce from the repository:

```bash
python scripts/astrogrid_edge_report.py --cache /path/to/archives \
  --output astrogrid_web/research/spot-flow-edge-v1.json
```

The [published JSON](../../astrogrid_web/research/spot-flow-edge-v1.json)
includes all archive URLs, hashes, sample filters/counts, per-asset/month and
reference results, paired intervals and cost sensitivity. The script writes
selected event evidence beside the local archive cache for independent audit;
it never inserts replay data into the live database. On Astrogrid, historical
assessment and the live scored cohort are labelled separately. Live cost
averages use original signal-close paper returns and overlapping forecasts;
they are explicitly diagnostic and insufficient for independent edge claims.

No trades, rule tuning, weight promotion, forecast rewrite or paid activation.
