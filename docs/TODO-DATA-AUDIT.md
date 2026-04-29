# Data Freshness Audit + Backfill Plan

**Status:** OPEN — needs a dedicated session
**Discovered:** 2026-04-29 while debugging the multi-horizon signal replay
**Owner:** Anik (priorities) + agent (execution)

## Why this exists

The multi-horizon replay backtester returned 0 rows because the price data
it needs to compute forward returns is stale or missing. Specifically:

- `raw_series` for `YF:ICLN:close` ends **2017-07-12** (almost 9 years stale)
- `raw_series` for `YF:AAPL:close` is current to **2026-04-17** (acceptable)
- **102 of 315** distinct tickers in `signal_registry` have **no `YF:<ticker>:close` series at all**
- `resolved_series` for AAPL is current to **2026-04-29** (yesterday)
- `oracle_predictions.actual_price` is set on scored rows for at least
  GLD, SPY, TLT, QQQ on 2026-04-28/29 — so SOMETHING is fetching prices,
  just not into raw_series for all tickers

The ingestion pipeline has 45+ modules per a partial code map. The freshness
audit (step62 SQL) timed out on the first query because `raw_series` is
huge and the nested GROUP BY pattern doesn't scale. We need targeted
queries per source instead of "scan everything".

This isn't a single-bug fix — it's a multi-step audit + fix campaign.

## Plan

### Phase 1 — Inventory (the audit, done right)

Goal: produce a structured table of every data source's actual freshness,
without melting the DB. Replace the original step62 nested-CTE approach
with targeted queries that hit indexes:

1. For `raw_series`, run **per-prefix** queries (FRED, YF, BLS, BCB, …)
   instead of one giant scan. The prefix-grouped query is what timed out;
   instead do `SELECT MAX(obs_date) FROM raw_series WHERE series_id LIKE 'YF:%'`
   for each known prefix. Should take seconds instead of minutes.
2. For each prefix, list the **5 oldest** `MAX(obs_date)` per series — those
   are the worst offenders.
3. For `resolved_series`, JOIN to `feature_registry` and rank by latest
   `obs_date`, grouping by family (vol / momentum / equity / rates / crypto).
4. For `signal_sources`, `signal_registry`, `news_articles`,
   `options_daily_signals`, `earnings_calendar`, `forensic_reports`: just a
   `MAX(date_col)` per source category.
5. **Coverage gap query**: tickers in `signal_registry` (last 7d) that have
   no `YF:<ticker>:close` row in `raw_series` within the last 7 days.
   This is the "we predict on tickers we can't price" set.
6. Read-side check: `SELECT subcategory, MAX(created_at) FROM analytical_snapshots`
   to see which ingestion modules have actually run recently. The hermes
   operator writes a snapshot per cycle, so absence of a recent snapshot
   for a given subcategory means that module isn't running.

Output: `docs/data-freshness-audit-2026-XX-XX.md` with one table per
source, sorted by staleness.

### Phase 2 — Map ingestion code to sources

For each stale source, find the module that's responsible:

1. `ingestion/yfinance_pull.py` writes `YF:*` series. Confirm via grep.
   If ICLN/CDNL/etc. are missing or stale, the puller's ticker list is
   probably out of date.
2. `ingestion/altdata/*.py` — each has its own target table. Match each
   stale source from Phase 1 to the file that writes it.
3. `ingestion/scheduler.py` — registers pullers on cadences. For each
   stale source, check whether its puller is registered AND whether the
   schedule fired recently (per the analytical_snapshots heartbeat from
   Phase 1 query #6).
4. Flag **orphaned modules**: ingestion files not registered in any
   scheduler. These are silently dead.

Output: append to the audit doc — `module → source → schedule → last_run`
table. Highlight the broken-vs-orphaned vs just-behind cases.

### Phase 3 — Backfill the gaps

For each stale source, pick the right action:

- **Just behind** (puller is scheduled and ran recently, but missed days):
  re-run the puller manually with a `--days N` argument to backfill the
  gap. Most modules support this.
- **Broken** (puller errors silently): read the module's last logged
  exception (probably in `/data/grid/logs/`), fix the code, run backfill.
- **Orphaned** (puller exists, never registered): register it in
  `ingestion/scheduler.py` with the right cadence, then run a one-time
  backfill.
- **Coverage hole** (ticker in signal_registry but never in raw_series):
  add the ticker to `ingestion/yfinance_pull.py`'s ticker list and run a
  full-history pull. Or stop generating signals for un-pricable tickers.

Output: for each source, a one-line bash command that backfills it. Run
them in priority order (highest-impact tickers first: SPY/QQQ/AAPL/MSFT
type tickers used by the conviction stack).

### Phase 4 — Verify the future

Once backfills run, verify the recurring schedule keeps them fresh:

1. Wait for the next scheduled run cycle.
2. Re-run Phase 1 queries.
3. Confirm `latest_obs` advances by the expected daily/hourly increment.
4. Add a **freshness alert** to the daily walk-forward report — if any
   high-value source goes stale by >7d, the report should yell.

### Phase 5 — Coverage expansion

The 102 signal_registry tickers without ANY YF series are a separate
problem. Three options:

1. **Add them to yfinance puller** — works for any ticker yfinance covers.
2. **Use a broader equity feed** — Polygon, Alpaca, Tiingo. We already
   have polygon API key per `.env`.
3. **Stop generating signals for un-pricable tickers** — defensive: filter
   `signal_registry` writes at the source if no price feed exists. Better
   to not have the signal than to have it without a way to validate.

Recommendation: do #1 first (low effort), then #3 as a safety filter.

## What NOT to do

- Don't paper over with fallbacks (e.g., "use last known price even if
  9 years stale"). The walk-forward replay would produce garbage numbers.
- Don't run `raw_series` full-table scans during DB pressure hours.
- Don't disable any ingestion modules until you've confirmed they're
  actually broken vs just slow.

## Prior work to lean on

- `scripts/walk_forward_profitability.py` — uses `oracle_predictions`,
  not `raw_series`. Already works.
- `scripts/signal_replay_backtest.py` — single-horizon path against
  `signal_sources` works (insider, congressional verdicts produced
  successfully). Multi-horizon path against `signal_registry` is what's
  blocked by this data audit.
- `oracle_predictions.actual_price` — proves SOMETHING is pulling fresh
  prices for at least the major tickers. Trace its source code to find
  the working pull path; that's the model for what we need everywhere.

## Acceptance criteria for "done"

- Every source listed in the Phase 1 audit has `latest_obs >= today - 7`
- Coverage gap (tickers with signals but no prices) drops below 10
- `signal_replay_backtest --days 180 --horizon 5` returns ≥10 cells
  across the full landscape (alpha_research, news_intel, feature:*, etc.)
- The daily walk-forward report includes a freshness section that
  flags any regression
