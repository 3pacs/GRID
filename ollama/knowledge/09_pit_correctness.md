# Point-in-Time Correctness

This is the most critical concept in GRID. Violating PIT correctness makes backtests worthless.

## The Problem

Economic data gets **revised**. The GDP number published on the first release is different from the number published 3 months later. If you use the final revised number in a backtest that's supposed to simulate decisions made at the time, you're cheating — you're using information that didn't exist yet.

## How GRID Solves It

Every data point in GRID has three dates:
- **obs_date**: When the economic event occurred (e.g., "January 2024 employment")
- **release_date**: When the data was actually published and available to traders
- **vintage_date**: Which revision of the data this is

## The Two Policies

### FIRST_RELEASE
Returns the **earliest revision** of each data point. This simulates what you would have seen the moment the data first came out.
- **Use for**: Backtesting — "what would I have known on this date?"
- **More conservative**: Real-world decisions were made on this data

### LATEST_AS_OF
Returns the **latest revision available** as of the decision date. If GDP was revised upward before your decision date, you get the revised number.
- **Use for**: Live inference — "what is the best estimate available right now?"
- **More accurate**: Uses all information available at the time

## The Hard Constraints

Every PIT query enforces:
```
release_date <= as_of_date   (can't use data not yet published)
obs_date <= as_of_date        (can't use future observations)
```

GRID runs `assert_no_lookahead()` on every query result as a safety net. If even ONE row violates these constraints, the entire query raises a ValueError.

## Why This Matters for Analysis

When interpreting backtest results:
- **FIRST_RELEASE backtests** are more realistic but may understate strategy performance (revisions often confirm the initial signal)
- **LATEST_AS_OF backtests** may overstate performance if revisions were systematically favorable
- **The gap between the two** tells you how much revision risk exists in a strategy

## Data Revision Patterns

Different series have different revision behaviors:
- **GDP**: Heavily revised (advance → preliminary → final, spread of 1-2% common)
- **Employment/NFP**: Revised once, usually within ±100K of initial
- **Market data**: Never revised (OHLCV is final)
- **CPI**: Rarely revised, very stable
- **Trade data**: Moderately revised, especially for large trading partners

GRID's `source_catalog` tracks revision behavior: NEVER, RARE, FREQUENT. This informs which vintage policy to prefer.
