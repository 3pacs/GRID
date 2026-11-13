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
| flow | dix, dix_change_5d | finra:dix_sp500: S&P 500 dollar-weighted dark-pool short share (`ingestion/altdata/finra_dix.py`, from 2018-08) | daily fast |
| positioning | dealer_gamma, spot_vs_gamma_flip | dealer_gex_daily (SPY; `physics/dealer_gex_daily.py`; registered captures from 2026-09-26) | daily fast |
| gamma_state | regime, gamma_flip, spot_vs_flip | latest dealer_gex_daily row | — |
| news | news_uncertainty, equity_news_uncertainty | USEPUINDXD, WLEMUINDXD: 7-day mean of days t-9..t-3, as FRED showed them on t | daily fast |
| news | news_first_print | USEPUINDXD: newest weekday first print, compared only with past first prints | daily fast |
| events | today, next, within_14d | FRED release dates (CPI, PPI, NFP, GDP, PCE, retail sales) + Fed calendar (FOMC) | — |
| news_shock | elevated_uncertainty / not_elevated | news_first_print z ≥ 2; calendar co-occurrence separate, attribution unverified | — |

Pending (listed in every record as `pending`): release surprise (no consensus source yet), earnings surprise.

A variable with some history but less than its baseline needs is `no_baseline`: value and observation date, no z or
label. Dealer gamma reads that way until ~120 sessions of registered captures exist (around spring 2027).

## News and vintages

The daily newspaper-based uncertainty indexes (Baker, Bloom, Davis) are revised for weeks: USEPUINDXD for
2022-06-02 first printed 60.23 and settled at 144.13. `fred_vintages` (`store/fred_vintages.py`, filled by
`ingestion/fred_vintages.py` from ALFRED) keeps every published version with the dates it was live, and answers
"what did FRED show for day X at the end of day D" for D within 45 days of X. Each past day's news reading is rebuilt
from the versions live on that day, so a first print is compared with past first prints, never with revised history.
First fill: `python -m ingestion.fred_vintages backfill` (vintages start 2014-03-27); the nightly writer refreshes it.

Checked on real data (read-only, 2026-11-13): 2020-03-16 and 2025-04-04/08 (tariffs) had elevated uncertainty
(z 5.0, 3.95, 5.0); 2022-06-13, the session after a hot CPI print, was not elevated (z −0.28).
These proxy readings do not themselves attribute the moves to news or establish that a session had no news.

## Availability

Every input goes through `store.observations.read_window_known_at` with a declared, deliberately late publication lag
(`market_state.LAGS`; documented shutdown releases override it). Backfilled history is revised vintage, not first
release. Monthly values therefore arrive a few weeks after their real release; that costs freshness, never adds
lookahead. Rows are insert-only; `--replace` is the only way to rewrite a date.

### News attribution boundary
The newspaper uncertainty proxy does not establish a market-moving shock, its cause, or a quiet/flow-only session. `calendar_context` reports only listed-release co-occurrence (or unknown when no calendar is loaded). `news_attribution` stays `unverified`; `supports_flow_only_classification` is false. Nonfinite or nonnumeric proxy scores are unavailable. The enclosing `news_shock` key is retained, but the new descriptive kinds intentionally replace the overclaiming scheduled/unscheduled/none categories. No production consumer currently reads those categories.

## DIX and dealer gamma

DIX_d = Σ ShortVolume×close / Σ TotalVolume×close over day d's S&P 500 members (`config/sp500_membership.json`, built
from fja05680/sp500 by `scripts/build_sp500_membership.py`), FINRA's CNMS file and same-day Tiingo closes. Share
weights track SqueezeMetrics' published DIX at only 0.90 correlation; dollar weights at 0.997 over 2,083 days. Checked
2026-11-05..11 against SqueezeMetrics to the third decimal. Backfill: `python -m ingestion.altdata.finra_dix --backfill
2018-08-01 <yesterday>`; nightly: the smart-scheduler entry `finra_dix`.

`dealer_gex_daily` (the godview table that had no writer) gets one SPY row per session from the latest registered
options capture completed by the end of that UTC day (`scripts/run_dealer_gex_daily.py`). Units: $M per 1 % SPY move.
Sessions without a registered capture, a spot receipt or a GEX zero crossing are skipped, not guessed.
