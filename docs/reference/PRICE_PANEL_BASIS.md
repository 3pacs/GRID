# `{ticker}_full` price panel: one basis per feature

Status: policy in code since 2026-11-08 (#1067, main `a4a4e284`). First repair dry run on
grid-svr 2026-11-08 23:11Z (65 features) reviewed: **not applicable as produced**, the
script's scope was wrong for three feature shapes (see "Scope of the repair"). Repair of
the rows written before the policy: planned, **not run** (needs Anik's explicit word).

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
(`_YF_FIELD_SUFFIXES`). It also refuses a derived name whose feature already has a static
feed from another ticker or another source (`pattern_collision()`): ticker `GOLD` is
Barrick Gold and derives `gold_full`, the gold-futures feature fed by `YF:GC=F:close`;
measured on griddb 2026-11-08, Barrick closes had resolved into `gold_full` since
2025-12-02 (first `YF:GOLD:adj_close` row, Tiingo). Refused rows stay unresolved and are
warned once per process. A ticker registered only as `<t>_full` that has no `adj_close`
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
* `normalization/entity_map.py` (follow-up): `static_feeds()`, `pattern_collision()` and the
  collision guard in `EntityMap._resolve_by_pattern` (same-named ticker, unrelated feature).
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

## Scope of the repair (what the first dry run taught, 2026-11-08)

The script derived every ticker from the feature **name**. Three feature shapes made that
wrong, and the follow-up takes the ticker from the entity-map feed instead
(`feature_scope()` in the script; `static_feeds()` in the entity map). Per-feature
`status` in the JSON, only `IN_SCOPE` plans are ever applied:

| status | which features | why |
|---|---|---|
| `OUT_OF_SCOPE` | `btc_full`, `eth_full`, `sol_full` (Binance/CoinGecko), anything whose static feed is not one `YF:<T>:<basis field>` series | never fed by the two yfinance fields, so the alternating-basis defect cannot exist there; a same-named yfinance ticker (bare `BTC`/`ETH` pulled as equities 2025-11..2026-05) is unrelated. The r1 run planned 96-97 % retraction and left them dark |
| `SKIPPED_NO_BASIS_SPLIT` | `RAW_CLOSE_FULL_FEATURES` except `spy_full`: `gold_full` (`GC=F`), `sp500_full`, `btc_usd_full`, `eth_usd_full`, `sol_usd_full` | no corporate actions, `adj_close == close`; a value matching one field only is pull-timing drift on a 24/7 or settlement bar, not a basis flip (r1 would have retracted 2,058 `eth_usd_full` rows for nothing). `gold_full` in r1 was compared against ticker `GOLD` = Barrick: 100 % retract, after-ratio 1.12 = Barrick, before-ratio 3.65 = gold |
| `NEEDS_BACKFILL` | in scope, but more than `--coverage-floor` (1 %) of today's served cells would have no on-basis raw row to re-vintage from: `acn_full`, `hon_full` (Tiingo `adj_close` starts 2021-04-01, ~half of the 2010+ cells) | **backfill first, repair second.** A backfill writes `YF:<T>:adj_close` rows with the backfill day's pull date; re-running the plan then re-vintages every dark cell from them. Until then these features are internally consistent raw-close series (the panel no longer runs the heuristic adjuster, so the residual error is dividend-sized). The JSON carries `backfill_hint` with the obs_date range |
| `EXCLUDED` | named with `--exclude` | operator's call, reported, never read or written |

Dark-cell accounting, per feature: `cells_dark_before` (no visible row today: every row
already retracted, typically the backdated fill rows retracted by
`scripts/audit_backdated_vintages.py`; r1's `left_dark` for `jnk_full` 961, `btc_usd_full`
1,228, `dia_full` 150 was entirely this), `cells_newly_dark` (served today **on a real
trading day**, i.e. a SUCCESS raw close row exists for the date, dark after the plan; the
number the floor judges), `cells_phantom_cleared` (served today on a date with no raw close
at all, dark after the plan: the holiday junk cells disappearing, which is the repair
working; r2 showed `xli_full` with 5,598 cells against ~4,250 trading days and 1,311 of them
going dark), `cells_restored` (dark today, re-vintaged by the plan), `cells_left_dark` (dark
after the plan, before + newly + phantom).

Re-vintage guard (follow-up): the r1/r2 plans refilled a dark holiday cell from the 0.6
junk print itself (it was the latest on-basis raw value for that date). A re-vintage now
needs a raw close row for the date and a value within 50 % of the neighbouring on-basis
cells; otherwise the latest sane candidate, or nothing, and `revintage_rejected` says why.
The r2 JSON's `revintage` lists for XLP/XLB/XLRE/XLI contain such junk refills and must not
be applied as written.

r2 totals (all 139 `_full` features, 2026-11-08 23:37Z → 00:21Z, nothing written): in scope
113 features, 6,114,750 rows, 1,784,025 retractions, 24,515 re-vintages, 9,354 cells left
dark; out of scope 26 features. `gold_full` alone had 155,619 rows flagged OUTLIER against
Barrick. In-scope after-ratios checked against real 2010 → 2026 adjusted prices: NVDA 440
(≈ $0.46 split-adjusted → ~$200), TSLA 225 (IPO 2010-06-29 $1.59 split-adjusted → ~$360),
GOOGL 18.3, AMZN 38.7, BRK-B 8.6, DUK 5.1, LOW 13.0, DIS 4.3, T 2.58 (dividends + WBD
spin-off), LQD 1.81, EMB 1.96, VZ 3.45 all land; MSTR 13.4 (post 10:1 split $8.5 → ~$115)
plausible; XOM 4.42, MRK 7.16, XLE 3.57 run 30-40 % above a rough dividend-adjusted
estimate and want a look at `served_after.first/last` before anyone quotes them.

The after-ratios r1 produced for in-scope equities with full `adj_close` coverage land on
the real 2010 → 2026 total-return ratios (GOOGL 18.3, AMZN 38.7, BRK-B 8.6, DUK 5.1, LOW
13.0, DIS 4.3); their before-ratios were the split/dividend artefact.

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

Reads: resolved rows by `feature_id` in one READ ONLY transaction, raw rows per
`(series_id, calendar-year)` chunk each in its own short READ ONLY transaction (r1 held one
connection 60-70 s per large feature). `--exclude <feature>` is repeatable;
`--coverage-floor` defaults to 0.01. A plan whose status is not `IN_SCOPE` is never
applied, whatever flags are passed.

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

## Open items (found by the dry-run review, 2026-11-08)

| item | evidence | owner / next step |
|---|---|---|
| `YF:HON:adj_close` has no SUCCESS row since **2026-04-17** while `YF:HON:close` continues | grid-svr read 2026-11-08: 1,267 rows 2021-04-01..2026-04-17, source TIINGO | puller: find why Tiingo's adj_close for HON stopped; check `pull_log` for every ticker whose `adj_close` max(obs_date) lags its `close`. Under the one-basis policy `hon_full` gets no new vintages until fixed |
| Barrick (`GOLD`) in the Tiingo daily list has fed `gold_full` since **2025-12-02** | `YF:GOLD:adj_close` 3,500 rows 2025-12-02..2026-11-06 (TIINGO); `YF:GOLD:close` 10,840 rows since 1985 (KAGGLE_BULK, TIINGO) | the fallback guard stops new rows; `gold_full` rows dated 2025-12 onward whose latest vintage is a Barrick value (~$20-40 against ~$4,000 futures) still need a targeted retraction (own run tag, `resolved_series_retractions`), not this script |
| `acn_full`, `hon_full` and any other `NEEDS_BACKFILL` feature | Tiingo `adj_close` backfill starts 2021-04-01 | backfill `YF:<T>:adj_close` 2010-01-01..2021-03-31 (Tiingo `tiingo-backfill`), then re-run the plan |
| the 150-cell dark group shared by `qqq_full`, `dia_full`, `xle_full`, `xlv_full`, `xlu_full` (r2) | one obs_date range with no `adj_close` raw row on every SPDR/QQQ | the follow-up script reports it as `cells_newly_dark` with the range (≈ 3 % of cells, above the 1 % floor → `NEEDS_BACKFILL`) or as `cells_dark_before`; backfill that range if the former |
| bare `BTC` / `ETH` / `SOL` written under the `YF:` namespace (yfinance 2025-11..2026-05, Tiingo 2026-03..04) | 122 / 21 rows, equity prices | both pullers exclude them today; rows are unmapped and harmless, delete nothing |

