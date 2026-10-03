# Intraday context controls: concurrent offline lanes

These three lanes add diagnostic context to the exploratory pressure lab. They
do not fit directional votes, inspect sealed outcomes, or demonstrate profit.
The original frozen preregistration and E2/E3 evaluation suites are unchanged.

| Lane | Measurement | Data required before market evaluation |
| --- | --- | --- |
| Time of day | Completed bucket activity / prior-session median, matching exchange-session offset | Calendar plus at least 20 completed prior sessions with original trusted receipts and consistent source |
| Sector participation | PIT weighted contributions, breadth, gross contribution concentration | Complete membership/weights known before interval start; synchronized constituent endpoints |
| Volatility repricing | Matched-contract midpoint change decomposed into spot, time, provider IV and carry, with residual and spread uncertainty | Synchronized quotes/spot/provider IV, contract identity and original trusted receipts |

Run all three suites concurrently from the repository root:

```powershell
python -B -m scripts.intraday_lab.run_context_tests --output C:/path/to/new-receipt-directory
```

The output directory must not exist. Separate logs and a JSON receipt record
lane start/end times, return codes, test/source hashes, interpreter and git head.
Missing files, timeouts and failed tests cannot produce PASS. These are synthetic
calculation/admission controls; PASS is not evidence of a profitable edge.

The runner uses three subprocesses, no project conftest, no database, and no
collector/service activation. Runner controls use a three-party barrier to verify
overlap and exercise failure, timeout and immutable-output behavior.

For edge research, preregister context stratification or interaction tests before
observing outcomes. Compare against price momentum on identical eligible
timestamps, split whole sessions chronologically, and measure executable SPYU
ask-to-bid returns with costs. Do not substitute ETF sector returns for missing
constituents, reconstruct historical receipt clocks, or treat provider IV as an
independent prediction. European decomposition is an approximation for American
ETF options. Until required market packets are admitted, all three remain
UNVALIDATED. No production integration is included in this PR.
