# Pre-registration: high-Sharpe ensemble v1 (walk-forward top-k over GRID's existing strategies)

Status: registered 2026-11-08 (the git commit that adds this file is the registration record).
Owner: Anik. Author: Claude (Fable 5.1), session `claude/high-sharpe-ensemble-20261108`.
Code: `trading/high_sharpe_ensemble.py` (pins this file's LF sha256 as `PREREG_SHA256`),
`scripts/high_sharpe_ensemble_backtest.py` (the look), `tests/test_high_sharpe_ensemble.py`.
Nothing below may change after the look is run. Any change is a new version (v2) with a new
file and a new pin; this file stays as it is. Paper only: the look reads the database through a
session forced `default_transaction_read_only=on`, writes no table, places no order, and
touches neither `decision_journal` nor `model_registry`.

## 0. What was seen before writing this

The inventory `docs/reports/HIGH-SHARPE-INVENTORY-20261108.md` (same commit) was written first.
It read GRID's stored records (paper trades, oracle verdicts, the 242-run rotation sweep,
journal outcomes, the forward logs) and two data-quality diagnostics of the price panel
(observation counts, first/last dates, counts of day-to-day jumps above 10 % and of jumps that
reverse within five sessions, and the first-to-last price ratio per ticker, by source). No
strategy return, Sharpe, drawdown or selection of any candidate below was computed on any
window before this registration. The one synthetic smoke test of the code (random-walk prices,
seed 1) is the only run of `walk_forward` that has happened.

Prior trials on this data that count against the result: the rotation sweep tried 240 parameter
sets of `adaptive_rotation` on 2024-01-01 → 2026-03-25 (all persisted, §1 row 2 of the
inventory). The seven candidates' own parameters were fixed by their authors (FinRL-X,
Kakushadze & Serur, QuantaAlpha, Baldisserri) on histories that overlap this window. The
deflated Sharpe therefore uses `n_trials = 248` = 240 sweep configurations + 7 candidates +
1 ensemble configuration. No other configuration of the selector will be run on this data.

## 1. Question

Does a walk-forward selection of the few best of GRID's existing rule-based strategies, ranked
by trailing out-of-sample Sharpe shrunk toward zero and penalised for mutual correlation,
weighted by inverse volatility, beat SPY buy-and-hold net of costs?

H1 (one-sided): the mean weekly net return of the ensemble minus the mean weekly return of SPY
buy-and-hold is positive over the out-of-sample segments.

## 2. Universe

Tickers (fixed): AAPL, MSFT, NVDA, META, AMZN, GOOGL, TSLA, XOM, CVX, GLD, SLV, XLE, TLT, XLU,
XLV, XLB, XLI, SPY, QQQ, IWM, DIA, XLF, XLK, XLP, XLY, IEF, SHY, HYG, LQD, USO
(`trading.high_sharpe_ensemble.UNIVERSE`). Sector set for the sector-rotation candidate:
XLE, XLU, XLV, XLB, XLI, XLF, XLK, XLP, XLY. Benchmark: SPY. Cash earns zero.

Candidate strategies (fixed; each is GRID code that existed before this registration, run with
its default parameters, and turned into target weights by the fixed construction named):

| id | GRID rule | construction |
|---|---|---|
| `adaptive_rotation` | `alpha_research.strategies.adaptive_rotation.run_rotation`, default constants, `positions={}` | the weights it returns (long only, cash remainder) |
| `s151_xs_momentum` | `trading.strategy151.Strategy151Engine.cross_sectional_momentum(formation=252, skip=21, top_n=5)` | LONG leg 0.5 gross equal weight, SHORT leg 0.5 gross equal weight |
| `s151_mean_reversion` | `Strategy151Engine.mean_reversion_scan(window=20, entry_z=2.0)` | same; no signal on a leg leaves that half in cash |
| `s151_sector_rotation` | `Strategy151Engine.sector_rotation(top_n=3)` on the sector set | same |
| `qa_dual_horizon_equity` | `alpha_research.signals.quanta_alpha.dual_horizon_equity` on the last 120 sessions | long top quintile, short bottom quintile, 0.5/0.5 gross equal weight |
| `qa_vol_regime_equity` | `alpha_research.signals.quanta_alpha.vol_regime_adaptive_equity` on the last 160 sessions | same |
| `vix_ma_exposure_spy` | VIX/MA(20) scalar `clip(1 − (VIX/MA − 1), 0, 1)` from `alpha_research.signals.exposure_scaler` | SPY weight = scalar, cash remainder |

Every candidate's gross exposure is capped at 1 (weights are rescaled if a rule returns more).

## 3. Data

Prices come from `raw_series` through `store.observations.read_window` (SUCCESS rows only, one
value per observation date, the latest pull), series `YF:{TICKER}:adj_close`, bounded by
`as_of` = the run date. The `{ticker}_full` features in `resolved_series` are **not** used:
the diagnostic in inventory §3 shows their vintages alternate between raw and dividend-adjusted
closes, and the universal split adjuster then compounds those flips into returns of 10^100 and
more (XLP, XLB, XLRE, XLI) and 2,070× for GLD. The 242-run rotation sweep read exactly that
panel, which is why every one of its configurations shows a +9,600 % return.

Source rule per ticker (mechanical, decided from observation counts only): use source `tiingo`
when its `adj_close` series holds at least 95 % as many observation dates as SPY's over the
loaded window, else source `yfinance`. A ticker is **excluded** from the universe for the whole
run, and listed in the look report, when its chosen series has (a) fewer than 90 % of SPY's
observation dates, (b) any observation dated on a Saturday or Sunday, (c) more than 25 reversed
jumps (a day-to-day move above 10 % followed within five sessions by an opposite move above
8 %; the diagnostic showed at most 16 such reversals on a genuine series, TSLA, and about 105 on
the corrupted yfinance XLP/XLB/XLRE series), or (d) its last observation more than 10 sessions
before the window end. Candidates run on
the surviving tickers; a candidate whose rule needs a missing ticker simply ranks what is there.

VIX: `vix_spot` through `alpha_research.data.panel_builder.get_vix_series` (PIT store,
`LATEST_AS_OF`, as of the run date). Gaps are forward-filled for at most 5 sessions.

Point-in-time statement. Every candidate decides on segment start `d` from prices and VIX
observed on or before `d` (`DecisionContext`; `PanelPITStore` refuses any read past `d`).
Realised returns use the prices that followed. The price vintages are those GRID held on the
run date: dividend-adjusted closes are restated by the vendor at each distribution, and
backfilled history carries its backfill date as `release_date`, so a strict release-date read
would see nothing before December 2025. This is the same convention every GRID backtest uses;
it is a walk-forward backtest, not a forward record. The forward record of this ensemble starts
with the look's date.

## 4. Selector and portfolio (all values fixed; `EnsembleConfig` defaults)

- Segments: weekly, decision on the last trading day of each `W-FRI` week; realised return from
  that close to the next segment's close. Window 2016-01-04 → 2026-11-06 (prices loaded from
  2015-07 for lookbacks).
- Candidate returns: `Σ w·r` over the segment minus `turnover × 10 bp` per side, turnover =
  `Σ |w_t − w_{t−1}|` (the first segment pays the full entry). A single-name segment return
  beyond ±50 % is a data fault: it is set to missing (counted and reported), never clipped.
- Selection, every 4 segments, from the trailing 52 segments of candidate net returns:
  annualised Sharpe (`mean / sd × √52`) shrunk by `n / (n + 52)`; greedy top-`k = 3` where
  each next pick maximises `shrunk Sharpe − 0.5 × max correlation with the picks so far`; only
  candidates with shrunk Sharpe > 0 are eligible; fewer than 3 (down to none, i.e. cash) when
  fewer qualify.
- Weighting: inverse trailing standard deviation over the selected candidates, summing to one.
  The ensemble's underlying book is `Σ_c a_c × w_c`; costs are charged once, on that book's
  turnover, at 10 bp per side. Gross exposure never exceeds 1.
- Warm-up: the first 52 segments produce candidate returns only. Out-of-sample segments are
  every segment after the warm-up, cash segments included (≈ 2017-01 → 2026-11, about 513
  weekly segments).
- Controls on the same segments and costs: SPY buy-and-hold (one entry cost), 1/N over all
  seven candidates re-weighted every 4 segments, and the best single candidate in hindsight
  (highest full-period Sharpe; it is a hindsight benchmark and is labelled as such).

## 5. One look, one criterion

The look is `python -m scripts.high_sharpe_ensemble_backtest --start 2016-01-04 --end 2026-11-06
--source raw-adj-close`, run once on grid-svr against the production database from a read-only
checkout of this branch at the registration commit or a later commit of the same branch, with
`PREREG_SHA256` matching this file. Its JSON and Markdown land in `docs/reports/` and are
committed to the branch as the record, naming the code commit they ran from.

Pass iff both hold:

1. The mean weekly net excess return of the ensemble over SPY buy-and-hold has a one-sided
   95 % circular-block-bootstrap lower bound above zero (10,000 resamples, seed 20261108, block
   length from `analysis.offline_research_proof.autocorrelation_block` on the excess series).
   α = 0.05, k = 1: this is the only selector configuration tried.
2. The ensemble's deflated Sharpe ratio (Bailey & López de Prado, as implemented in
   `alpha_research.validation.gauntlet.deflated_sharpe_ratio`) exceeds 0.95 at
   `n_trials = 248`, with the cross-trial Sharpe variance taken over the seven candidates'
   per-period Sharpes and the ensemble's own skewness and kurtosis.

Everything else is descriptive and may not rescue a FAIL: annualised Sharpe, return, maximum
drawdown and turnover of the ensemble and the three controls; per-candidate Sharpe, drawdown,
turnover and a Bonferroni (α/7) bootstrap lower bound on each candidate's excess over SPY;
the probability of backtest overfitting over the 7-column candidate matrix (CSCV, 16
partitions, `alpha_research.validation.overfitting`); selection counts and the selection
history. Win rate is reported but means nothing.

## 6. What a result means

- PASS: the ensemble earns a shadow paper book (`paper_trades`, a new strategy id, no capital)
  and a `model_registry` CANDIDATE row through the normal governance path, in a separate
  reviewed change; promotion is not allowed by this document. A pass on 2016–2026 is still a
  backtest whose candidates saw parts of this history; the forward record decides.
- FAIL: no shadow book. The result is recorded and the candidates stay where they are. A new
  selector or a changed candidate set is v2, registered before any further look.
- Either way, the look is reported to Anik with the numbers and the faults, and nothing is
  merged into the live trading paths (`GRID_ENABLE_*` flags untouched).

## 7. Integrity

- `PREREG_SHA256` in `trading/high_sharpe_ensemble.py` must equal `lf_sha256` of this file;
  `tests/test_high_sharpe_ensemble.py` checks it and that every candidate id and knob above
  appears in this text.
- The look records: this file's sha256 (pinned and observed), the commit, the host, the price
  source and exclusions per ticker, the panel shape and date range, the number of PIT reads,
  candidate failures per date, and the count of missing segment returns.
- The data pull runs through `store.observations.read_window` and `store.pit` only, with
  `default_transaction_read_only=on` and a 120 s statement timeout; no `raw_series` IN-list
  queries.

## 8. Deviations (declared)

- Prices are vendor dividend-adjusted closes (total-return basis); cash earns 0 %. Both favour
  SPY buy-and-hold relative to strategies that hold cash, which is the conservative direction
  for H1.
- The weekly `W-FRI` grid lets a candidate that would trade daily (mean reversion) act only
  weekly. That is the registered construction, not the authors' original cadence.
- Strategy151 is documented as reading a `market_daily` table that does not exist in the repo;
  its scan functions are pure functions of a price panel and are called that way here.
