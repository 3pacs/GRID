# Pre-registration: high-Sharpe ensemble v6 — regime-conditional selection from external factors, with a long-sample check

Status: registered 2026-11-08 (the git commit that adds this file is the registration record).
Owner: Anik. Author: Claude (Fable 5.1), session `claude/high-sharpe-ensemble-20261108`.
Opened under Anik's 2026-11-08 decisions, relayed in three parts: every further set must be
regime-specific using external factors; the deliverable is the picker's choice for *today's*
regime, with a long sample used to validate the conditioning (1946–48 and 1973–82 as separate
out-of-sample regimes), not to trade the 1940s; and an energy supply-shock axis with the oil
windows 1973–74, 1979–81, 1990, 2022 and June 2025. Code: `trading/regime_ensemble.py`,
`alpha_research/data/long_sample.py`, the `regime=` hook in `trading/high_sharpe_ensemble.py`
(`REGISTRATIONS["v6"]`, `candidate_set("v6")`), `scripts/regime_ensemble_look.py`,
`tests/test_high_sharpe_ensemble.py`. Nothing below may change after the look; a change is v7.
Paper only; read-only database session; no governance row above SHADOW, none unless PASS.

## 0. What was seen before writing this

Sets v1–v5 are spent (all FAIL; inventory §4–§9: nothing beats SPY on excess return after costs;
thirteen arms beat SPY's Sharpe; the trailing-Sharpe selector lost to 1/N in every set). What
exists in GRID for regimes was mapped and is **not** reused, for stated reasons: `scripts/
auto_regime.py` (GROWTH/NEUTRAL/FRAGILE/CRISIS from a weighted stress z-score; needs the database
and a PRODUCTION model row; history from 2026), `intelligence/regime/` (24-dimension state
vector and a five-axis classifier; database-bound; `raw_series` history from 1990),
`discovery/clustering.py` (fitted GMM, arbitrary cluster ids), `alpha_research/signals/
credit_cycle.py` (two states; reads vintages that cannot see history), `adaptive_rotation.
detect_regime` (SPY trend and VIX, i.e. the strategies' own prices). The one reusable idea is
the modelled publication lag (`store.observations.PublicationLag`, `intelligence/regime/
state_vector.PUBLICATION_LAGS`: INDPRO 50 days, UNRATE 42); those values are adopted.

Before this file, the following was looked at, and nothing else: griddb's FRED coverage
(every series starts 1990-01, `release_date` = backfill date 2026-03/04, so a strict PIT read
sees nothing before 2026); the layout and spans of the Kenneth French files, the FRED
`fredgraph.csv` long series and the GPR workbook; and **the regime labels themselves** on the
real long series — cell counts over 1946–2026, the cells inside the analog windows, the labels
since 2024, and today's cell. No strategy return, Sharpe or selection conditional on any cell
was computed. The code ran on synthetic panels only, plus one assembly check of the long-sample
panel that printed the raw annualised returns of the panel columns (market, bills, bonds,
industries, factors, oil) over the 1946–48 window and the cells in force there — descriptive
inputs to §5, not an arm or ensemble result. The current cell as known on 2026-11-07 is
`expand_reflate` (§6 states the numbers); 2026-03 to 2026-06 were `oil_shock`.

**Cumulative trials.** 280 before this set. Every arm × cell is a trial: 27 ETF arms × 6 cells
+ 8 long-sample arms × 6 cells + 2 regime-conditional ensembles (ETF era, long sample) = 212, so
**492 cumulative trials** for the deflated Sharpe.

## 1. Question

Conditioned on an externally defined macro regime known at the decision date, does picking the
top-3 of GRID's registered weekly arms by their trailing Sharpe *inside that regime* beat SPY
buy-and-hold in **today's regime cell**, and does the same picker, applied to documented
rule-based arms on a 1926–2026 monthly sample with the same cells, agree in the analog cells
rather than contradict it?

## 2. Regime cells (fixed; `trading.regime_ensemble`)

Inputs, all external to the strategies, monthly, FRED ids: `CPIAUCNS` (CPI-U NSA, 1913–),
`INDPRO` (1919–), `BAA` and `AAA` (Moody's yields, 1919–), `WTISPLC` (WTI spot, monthly, 1946–);
descriptive only: the Caldara–Iacoviello historical GPR index (`GPRH`, 1900–). Derived factors:
CPI y/y = 12-month log change; INDPRO y/y likewise; Baa−Aaa spread; WTI 6-month log change.

| cell | rule (precedence top to bottom) |
|---|---|
| `stress` | Baa−Aaa above its trailing 120-month 80th percentile (min 36 months of history) |
| `oil_shock` | WTI 6-month log change above +0.35 |
| `expand_reflate` | INDPRO y/y > 0 and CPI y/y above its value 12 months earlier |
| `expand_disinflate` | INDPRO y/y > 0 and CPI y/y at or below 12 months earlier |
| `contract_reflate` | INDPRO y/y ≤ 0 and CPI y/y rising |
| `contract_disinfl` | INDPRO y/y ≤ 0 and CPI y/y falling |

A month's label becomes known 50 days after the first of that month (the slowest input,
INDPRO's modelled lag; CPI 45, Baa/Aaa and WTI 35). The walk-forward uses, at every decision
date, the latest label known on or before it. Values are latest-vintage (CPI and INDPRO are
revised; GRID stores no ALFRED vintages), which is declared as the limitation it is.
GPR above its trailing 120-month 90th percentile is reported alongside, not used as a cell.

## 3. Data

ETF era: exactly v1 §3 (Tiingo `adj_close` through `store.observations.read_window`, the same
source rule and screen, vintages as of the run date), universe `UNIVERSE_V5` (33 tickers).
Long sample, one-off documented downloads cached with a sha256 manifest
(`alpha_research.data.long_sample`), nothing written to the database:

- Kenneth French data library: `F-F_Research_Data_Factors_CSV.zip` (Mkt-RF, SMB, HML, RF, monthly
  1926-07–), `F-F_Momentum_Factor_CSV.zip` (1927-01–), `10_Industry_Portfolios_CSV.zip`
  (value-weighted, 1926-07–). Total-return indices: `MKT` = Mkt-RF + RF, `RF` = bills, `MOM` and
  `HML` = factor long-short return plus bills, the ten industries by name.
- FRED `fredgraph.csv` (keyless, the endpoint `scripts/bulk_data_downloader.py` uses): the five
  regime inputs plus `TB3MS`, `GS10`, `LTGOVTBD`, `FEDFUNDS`, `TWEXBMTH`, `DTWEXBGS`, `PPIACO`,
  `UNRATE`. `BOND` = total-return index from yields (`LTGOVTBD` 1925–1953-03 with duration 10,
  then `GS10` with duration 7; monthly return ≈ y/12 − D·Δy, a declared approximation).
  `OIL` = `WTISPLC` level. Gold: FRED's London fixing series is discontinued; gold is reported in
  the ETF era (GLD) only.
- GPR: `data_gpr_export.xls` from the authors' site.
- Cross-check: the downloaded `INDPRO` and `FEDFUNDS` are compared with griddb's FRED rows from
  1990 and the maximum absolute difference is reported.

Tradability: the French portfolios and factors are not directly tradable. Proxies in the ETF
era are the SPDR sectors (industries), SPY (market), SHY/BIL (bills), IEF/TLT (duration); the
long-short factors `MOM` and `HML` have no cheap proxy and are reported as research arms. The
long sample charges 20 bp per side on turnover and holds bills, not zero-yield cash, when an arm
is out of the market (the ETF era keeps v1's zero-yield cash).

## 4. Arms, selector, cadence, costs

ETF era (`candidate_set("v6")`): all 27 weekly arms of v1, v2 and v3, unchanged; weekly `W-FRI`
segments 2016-01-04 → 2026-11-06; 10 bp per side; v1's selector (trailing 52 segments, shrunk
Sharpe `SR × n/(n+52)`, top-3, 0.5 correlation penalty, inverse vol, re-select every 4) with one
change: **the trailing window is the last 52 segments observed in the cell in force at the
selection date**; a cell with fewer than 26 observed segments falls back to the unconditional
trailing-52 window and the record says so (`regime_min_obs = 26`). Control: the same 27 arms with
the unconditional selector. Also reported: SPY buy-and-hold, 1/N over the 27 arms, best arm.

Long sample (`trading.regime_ensemble.long_sample_candidates`, `LS_CONFIG`): monthly segments
(month end), walk-forward from 1931-01 (first selection after 60 months), 20 bp per side,
trailing 60 months in the cell in force, shrink `n/(n+24)`, top-3, 0.5 correlation penalty,
inverse vol, re-select every 3, `regime_min_obs = 24`; before 1946-07 no cell exists (WTI
starts 1946) and the selector is unconditional. Eight arms:

| arm | definition |
|---|---|
| `ls_mkt_tsmom_12_1` | market if its 12-1 month return is positive, else bills |
| `ls_mkt_ma10` | market if above its 10-month mean, else bills |
| `ls_industry_mom_top3` | top 3 of the 10 industries by 12-1 return, equal weight |
| `ls_mom_factor` | the momentum factor plus bills, 100 % |
| `ls_hml_factor` | the value factor plus bills, 100 % |
| `ls_energy_tilt` | 50 % market, 50 % energy industry |
| `ls_bonds` | the bond index, 100 % |
| `ls_6040` | 60 % market, 40 % bonds |

Benchmark in the long sample is the market (`MKT`).

## 5. Analog windows (fixed)

Reported as separate cells of the long sample with their sizes, the cells in force inside them,
annualised returns of market, bills, bonds, energy (and energy − market), the momentum and
value factors and oil, and the regime-conditional ensemble against the market inside the window:

| window | months | role |
|---|---|---|
| 1946-01 → 1948-12 | post-war disinflation under heavy debt | lead analog for today |
| 1973-01 → 1982-12 | the great inflation | the failure mode to rule out |
| 1973-10 → 1974-12 | embargo, Fed accommodated | oil shock |
| 1979-01 → 1981-12 | Iranian revolution, Volcker tightened | oil shock |
| 1990-08 → 1991-03 | Gulf War spike | oil shock |
| 2021-03 → 2023-12 | post-shock inflation | current-era inflation |
| 2022-02 → 2022-12 | Russia | oil shock |
| 2025-06 → 2025-07 | Israel–Iran spike | current-era event (2 months; descriptive only) |

The dollar (`TWEXBMTH`/`DTWEXBGS`, 1973–) and GLD (2004–) are reported where they exist. The
hypothesis the record must check rather than assume: a Hormuz-type disruption today should hurt
importers more than a net-exporting US, firm the dollar, favour US energy and trend arms and
hurt duration first, with the Fed's response as the fork between the 1973 and 1979 paths. The
look reports what each window's data says against each clause.

## 6. Today's regime and what passes

As known on 2026-11-07 (observation month 2026-09): CPI y/y 3.67 % against 2.97 % a year earlier
(rising), INDPRO y/y +2.2 %, Baa−Aaa 0.44 against a stress threshold of 1.04 (calm credit), WTI
6-month log change +0.06 (the spring-2026 shock has rolled off), GPR 137 above its 10-year 90th
percentile of 128. Cell: **`expand_reflate`** — growth positive with inflation re-accelerating.
Nearest analogs by cell and narrative: 1946–48 (post-shock disinflation under heavy debt, mostly
`expand_disinflate` with `expand_reflate` months — the lead candidate, and a reminder that the
label can flip quarter to quarter), 1973–82 (the ratchet to rule out), 2021–23.

The look is `python -m scripts.regime_ensemble_look --start 2016-01-04 --end 2026-11-06`, once,
on grid-svr from a read-only checkout of this branch at this commit or later, pin matching.
**PASS iff** (a) the watch criterion holds **inside today's cell in the ETF era**: the ensemble's
annualised Sharpe on the cell's segments exceeds SPY's on the same segments, the one-sided 95 %
block-bootstrap lower bound of the mean weekly net excess over SPY on those segments is above
zero, the deflated Sharpe probability of the whole ETF-era ensemble exceeds 0.5 at 492 trials,
and the ensemble's drawdown on the cell's segments is no worse than SPY's; **and** (b) the long
sample does not contradict it: the long-sample ensemble's mean monthly excess over the market is
non-negative in the same cell and in the 1946–48 window. A cell with fewer than 52 weekly
segments is reported as too thin to read, whatever it shows. Everything else — the unconditional
control, 1/N, per-cell tables, per-arm records in today's cell (a), the analog cells (b) and
overall (c), PBO, selection history and fallback share — is descriptive.

## 7. What a result means

PASS → the regime-conditional ensemble gets a SHADOW paper book and a `model_registry` row no
higher than SHADOW in a separate reviewed change; its forward record starts in today's cell and
the registration names the cell it was admitted under. FAIL → no book, no row; the record still
answers Anik's question (which arms have a record in today's cell and in its analogs) and any
further set is a new registration at ≥ 493 trials. The look is spent either way.

## 8. Integrity and declared deviations

- `REGISTRATIONS["v6"]` pins this file; the tests assert the 492 count, the six cells and the
  eight long-sample arms by name, the lag of the known-at dates, and that the engine's
  conditional selection uses in-cell history and records fallbacks.
- Latest-vintage macro inputs with modelled lags, not first prints; a 50-day lag is conservative
  for CPI but may be a week late for INDPRO's first estimate.
- The month-end rebalance of the long sample and the weekly grid of the ETF era are not the same
  clock; the regime label is the same series read at each clock.
- Long-sample costs (20 bp) and cash (bills) differ from the ETF era (10 bp, zero cash) by
  design and are declared; nothing is compared across the two samples except the sign of the
  excess in a cell.
- The analog windows are named before the look; their sizes are small (the 2025-06 window is two
  months) and are reported as such.
