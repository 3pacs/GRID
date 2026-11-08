# `{ticker}_full` price panel: one basis per feature

Status: policy in code since 2026-11-08 (branch `claude/fix-full-panel-vintages-20261108`).
Repair of the rows written before it: planned, **not run** (needs Anik's explicit word).

## The defect (measured read-only on grid-svr, 2026-11-08)

`normalization/entity_map.py` mapped both `YF:<T>:adj_close` and `YF:<T>:close` onto
`<t>_full`. `normalization/resolver.py` resolves one `(series_id, obs_date)` group at a
time and inserts one row per pull-date vintage with `ON CONFLICT (feature_id, obs_date,
vintage_date) DO NOTHING`, so:

* on a day both fields were pulled, the two series raced for the same key and whichever
  worker's insert ran first won (partition order, not priority);
* on a day only one field was pulled (the scheduled yfinance run is close-only for most
  tickers, Tiingo's routine pull writes both), that field won alone.

Successive vintages of one observation date therefore alternated between the
dividend/split-adjusted close and the as-traded close (SPY 2016-01-04: 201.02 on most
vintages, 169.91 on the 2026-04-24 and 04-29 vintages; NVDA 2021-07-19: 751.19 vs 18.74).
`PITStore.get_pit(..., "LATEST_AS_OF")` serves the latest vintage per date, so the panel
jumped by the adjustment ratio wherever the latest vintage's basis differed from its
neighbour's. `alpha_research/data/split_adjuster.adjust_panel` then read every drop
beyond -40 % as a split and multiplied all earlier history by it, never undoing the
reversal. First-to-last ratios actually served as of 2026-11-07
(`docs/reports/HIGH-SHARPE-INVENTORY-20261108.md` §3):

| ticker | served | plausible | day jumps > 10 % |
|---|---|---|---|
| XLP | 8 x 10^201 | ~2.5 | 101 |
| XLB | 9 x 10^134 | ~2.6 | 98 |
| XLRE | 1 x 10^148 | ~1.8 | 98 |
| GLD | 2,071 | 3.3 | 2 |
| NVDA | 366 | ~60 | 23 |

XLP/XLB/XLRE also carry ~0.6 junk prints that a yfinance writer stamped on market
holidays into `YF:<T>:adj_close` (pulled 2026-06-25); each one is a -99 % day the adjuster
treated as a ~130:1 split.

Consumers of this panel: `alpha_research/strategies/adaptive_rotation.py`, the
`rotation_variant` fleet sweep, QuantaAlpha/LightGBM rank signals, `realized_alpha`'s SPY
path. Their 2026 numbers are artefacts (see "Downstream records" below).

## The policy

**Exactly one raw price field feeds a `_full` feature.**

| feature | basis series | why |
|---|---|---|
| every equity / ETF `<t>_full` | `YF:<T>:adj_close` (Tiingo `adjClose`, Yahoo `Adj Close`) | split- and dividend-adjusted as of each vintage's pull; Tiingo's series is clean and complete for the 30-ticker universe (2,856 NYSE dates 2015-07-01 → 2026-11-06, sane ratios: SPY 4.24, AAPL 12.1) |
| `spy_full` | `YF:SPY:close` | pinned by `price_close_contract.py` (`SPY_CLOSE_FEATURE`): the receipted **unadjusted** SPY close written only by `Resolver._resolve_spy_close_receipt`; `YF:SPY:adj_close` is no longer mapped at all |
| `sp500_full`, `gold_full`, `btc_usd_full`, `eth_usd_full`, `sol_usd_full` | `YF:<T>:close` | indices, futures, crypto: no corporate actions, Yahoo's adj_close equals close |

The allowlist is `normalization.entity_map.RAW_CLOSE_FULL_FEATURES`.
`price_basis_conflicts()` returns every feature that violates the policy over the static
mappings; `tests/test_full_panel_basis.py` asserts it is empty and the resolver logs a
warning per run if an `EntityMap` reports a violation (`mixed_basis_features()`).

The dynamic `YF:<ticker>:<field>` fallback follows the same rule: `adj_close` may derive
`<t>_full` / `<t>_adj_close`, `close` may derive `<t>_close` only
(`_YF_FIELD_SUFFIXES`). A ticker registered only as `<t>_full` that has no `adj_close`
rows therefore receives nothing; that is the honest state, not a silent raw-basis feed.
Tiingo's routine pull writes `adj_close` for the oracle close universe plus the
hypothesis tickers; the yfinance full-history pulls write it for everything they cover.
`scripts/repair_full_panel_vintages.py` (dry run) reports per feature how many raw rows of
each field exist, which is how to find a `_full` feature that now lacks a feed.

## What changed in code

* `normalization/entity_map.py`: the `close → *_full` comprehensions and the static
  `close → *_full` lines (HYG, LQD, TLT, TSM, KRE, XBI, BRK-B, QQQ) are gone; TSM/KRE/XBI/
  BRK-B gained `adj_close` mappings; crypto keeps `close` only; `YF:SPY:close → spy_full`
  is explicit; `RAW_CLOSE_FULL_FEATURES`, `FULL_PRICE_BASIS_FIELD`,
  `price_basis_conflicts()`, `EntityMap.mixed_basis_features()`.
* `normalization/resolver.py`: run-time tripwire (warning) when the map feeds a feature
  from two bases.
* `alpha_research/data/panel_builder.py`: `build_price_panel` no longer calls the
  heuristic `adjust_panel`. It reads values **and vintage dates** through `get_pit` and
  re-bases each cell from its vintage's share basis to `as_of_date`'s with the recorded
  split events (`YF:<T>:split_factor` via `store.observations.read_splits`, PIT by
  `as_of`): `value / Π ratio(splits in (vintage_date, as_of_date])`. A cell whose vintage
  already carries every split is unchanged, so the transform is idempotent per vintage
  and never infers anything from the prices themselves. `split_rebase=False` turns it off.
* `alpha_research/data/split_adjuster.py`: still available to callers with unlabelled
  series (`conviction_scorer`, `compute_real_drawdown`), but `detect_splits` now refuses a
  drop that climbs back above the threshold line within 10 calendar days (5 bars for a
  non-datetime index): a glitch, not a corporate action, logged and skipped.

## Known limitations of the adj_close basis

* **First-pull semantics.** `ingestion/tiingo_pull.py::_insert_rows` writes a
  `(series_id, source_id, obs_date)` only when no SUCCESS row exists, so a Tiingo
  `adj_close` value is the adjustment as of the first pull of that date, never refreshed.
  Dates backfilled in one bulk pull share one dividend basis; dates pulled daily since are
  effectively unadjusted for dividends paid after them. The drift is the dividend yield
  between pull dates (~1-2 %/yr for ETFs), with a step at the backfill boundary. yfinance
  re-pulls append fresh vintages, so LATEST_AS_OF refreshes where yfinance covers.
* **Splits after the first pull** are not in the stored adj_close of earlier dates; the
  panel's recorded-split re-basing handles exactly that case. Dividends are not re-based.
* `spy_full` is a raw close next to 29 adjusted series in the rotation universe: SPY's
  dividend drag (~1.3 %/yr) is a small known bias in any SPY-relative number read from the
  panel. Keep it: the astrogrid receipt contract needs the unadjusted print.

## Repair plan for rows already written

`scripts/repair_full_panel_vintages.py` -- dry run by default, writes nothing.

```bash
python scripts/repair_full_panel_vintages.py --feature xlp_full --feature gld_full --feature nvda_full
python scripts/repair_full_panel_vintages.py --all-full-features --max-features 60 --out /tmp/full-panel-plan.json
```

Per feature it classifies every `resolved_series` row by matching the value against the
SUCCESS raw rows of the two fields for that date: `ON_BASIS`, `OFF_BASIS`, `AMBIGUOUS`
(equal values, kept), `UNMATCHED` (kept, reported), `OUTLIER` (> 50 % from the median of
the neighbouring on-basis cells, the junk prints). It reports the served series before
and after (first-to-last ratio, day jumps > 10 %), the cells that would go dark and how
many of them can be re-vintaged from an on-basis raw row. Reads are bounded (resolved by
`feature_id`; raw by `(series_id, obs_date)` yearly chunks), READ ONLY, with a statement
timeout, and refused inside the 03:30-10:30 UTC DB window unless `--allow-db-window`.

`--apply --run-tag <tag>` (only on Anik's explicit word; Root/Anik decide):

1. inserts one `resolved_series_retractions` row per OFF_BASIS/OUTLIER row (append-only;
   the FK keeps the retracted row; reads with `as_of` before the retraction day are
   unchanged, reads from that day on no longer see it);
2. for a cell left without a visible vintage, inserts ONE new `resolved_series` row dated
   the repair day with the latest on-basis raw value (`ON CONFLICT DO NOTHING`);
3. nothing is UPDATEd or DELETEd; a cell with no on-basis raw row stays dark and is counted.

After applying, re-run the dry run: `served_after` ratios should be plausible and
`jumps_gt_10pct` near zero; then `build_price_panel` for the 30-ticker universe should
match Tiingo `adj_close` read through `store.observations.read_window` to within the
first-pull dividend drift above.

## Downstream records to re-label as invalid (owners act; the script touches none)

| record | producer | why | action |
|---|---|---|---|
| `astrogrid.backtest_run` / `backtest_result`, `run_key rotation_variant_*` (242 runs, 2026-05-08 → 05-19) | `alpha_research/strategies/rotation_variant_backtest.py`, fleet job `rotation_variant` | Sharpe 4.5-4.7 and +9,600 % on every config are the vintage-flip artefact | mark invalid; re-run only after the repair, under the gate in `docs/paper_log/high-sharpe-ensemble-v1-preregistration.md` |
| `paper_trades` strategy `adaptive_rotation_live` (wallet `grid_rotation_paper_7913a6f5`) | `adaptive_rotation.py` via `scripts/rotation_paper_trader.py` | signals from the corrupted panel; 19 of 23 trades at a flat price | no usable record; exclude from performance tables |
| QuantaAlpha / LightGBM rank signals and their 2026 logged scores | `alpha_research/signals/quanta_alpha.py` over `build_price_panel` | ranks computed on the mixed-basis panel | owner re-labels the 2026 runs; recompute after the repair |
| `realized_alpha_daily` rows with `spy_feature = spy_full` | `alpha_research/realized_alpha.py::load_spy_path` | `spy_full` carried adjusted vintages (2026-04-24/29) beside the receipted raw close | re-score affected windows after the repair; keep old rows (the table is a log) |

The same list is in the script's JSON output under `downstream`.
