# Market-state record (market-state/1)

One machine-readable row per session in `market_state_daily`: every state variable reported against its own trailing
baseline, so each reads as "this far above or below its normal". Code: `intelligence/regime/market_state.py`
(record), `intelligence/regime/baseline.py` (rule trailing-robust/1), `scripts/run_market_state.py` (writer),
`config/econ_release_dates.json` (scheduled events, built by `scripts/build_econ_release_dates.py`).

## Baseline rule (trailing-robust/1)

| Part | Rule |
|---|---|
| Window | the variable's own previous observations, strictly before the one read |
| Window length | daily fast 252 obs (min 120), daily slow 1,260 (min 500), weekly 260 (min 104), monthly 60 (min 36) |
| Robust z | (value − median) / (1.4826 × MAD), capped ±5; IQR/1.349 when MAD is 0; none when both are 0 |
| Percentile | mid-rank in the window, 0..1 |
| Label | normal \|z\|<1 · above/below 1–2 · high/low ≥2 (from the percentile when there is no z) |
| Too little history | `unavailable` with the reason, never a guess |

## Variables (v1)

| Group | Variable | Input | Window |
|---|---|---|---|
| volatility | vix, vix_term (VIX/VIX3M) | CBOE:VIX, CBOE:VIX3M | daily fast |
| credit | hy_spread, ig_spread | BAMLH0A0HYM2, BAMLC0A0CM | daily fast |
| rates | yield_curve, fed_funds, real_fed_funds | T10Y2Y, DFF, DFF − T5YIE | daily slow |
| liquidity | net_liquidity, net_liquidity_4w | WALCL − WTREGEN − RRPONTSYD×1000 | weekly |
| labor | initial_claims; unemployment, payrolls_3m | ICSA; UNRATE, PAYEMS | weekly; monthly |
| inflation / growth | cpi_yoy, industrial_production_yoy, consumer_sentiment | CPIAUCSL, INDPRO, UMCSENT | monthly |
| trend | spy_vs_50d, spy_vs_200d | SPY close (spy_full, else YF:SPY:close) | daily fast |
| flow | short_volume_ratio | finra:short_volume_agg:market_ratio (GRID history from 2025-11) | daily fast |
| news | news_uncertainty, equity_news_uncertainty | USEPUINDXD, WLEMUINDXD: 7-day mean of days t-9..t-3, as FRED showed them on t | daily fast |
| news | news_first_print | USEPUINDXD: newest weekday first print, compared only with past first prints | daily fast |
| events | today, next, within_14d | FRED release dates (CPI, PPI, NFP, GDP, PCE, retail sales) + Fed calendar (FOMC) | — |
| news_shock | kind = unscheduled / scheduled / none | news_first_print z ≥ 2, scheduled when a release fell on the printed day | — |

Pending (listed in every record as `pending`): release surprise (no consensus source yet), earnings surprise, DIX,
dealer gamma.

## News and vintages

The daily newspaper-based uncertainty indexes (Baker, Bloom, Davis) are revised for weeks: USEPUINDXD for
2022-06-02 first printed 60.23 and settled at 144.13. `fred_vintages` (`store/fred_vintages.py`, filled by
`ingestion/fred_vintages.py` from ALFRED) keeps every published version with the dates it was live, and answers
"what did FRED show for day X at the end of day D" for D within 45 days of X. Each past day's news reading is rebuilt
from the versions live on that day, so a first print is compared with past first prints, never with revised history.
First fill: `python -m ingestion.fred_vintages backfill` (vintages start 2014-03-27); the nightly writer refreshes it.

Checked on real data (read-only, 2026-11-13): 2020-03-16 and 2025-04-04/08 (tariffs) read as unscheduled news shocks
(z 5.0, 3.95, 5.0); 2022-06-13, the session after a hot CPI print, reads "none" (z −0.28): a scheduled release with
no news spike.

## Availability

Every input goes through `store.observations.read_window_known_at` with a declared, deliberately late publication lag
(`market_state.LAGS`; documented shutdown releases override it). Backfilled history is revised vintage, not first
release. Monthly values therefore arrive a few weeks after their real release; that costs freshness, never adds
lookahead. Rows are insert-only; `--replace` is the only way to rewrite a date.
